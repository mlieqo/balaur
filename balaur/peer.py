from typing import List, Optional, Set

import asyncio
import uuid
import itertools
import logging

import message
import torrent
import udp_tracker
import protocols
import piece


class PeerManager:

    MAX_CONNECTIONS = 20
    REFRESH_PEERS_DELAY = 120

    def __init__(self, torrent: torrent.Torrent, peer_id: bytes, file_queue):
        self._peers: Optional[Set['Peer']] = None
        self._active_peers = set()
        self._torrent = torrent
        self._peer_id = peer_id
        self._file_queue = file_queue
        self._message_handler = message.MessageHandler()
        self._workers: Optional[List[PeerWorker]] = None
        self._active_peers_available: Optional[asyncio.Event] = None
        self._peer_refresh_task: Optional[asyncio.Task] = None
        self._logger = logging.getLogger(self.__class__.__name__)

    async def start(self):
        self._active_peers_available = asyncio.Event()
        await self._load_peers()

        self._workers = [
            PeerWorker(
                self._peers,
                self._active_peers,
                self._peer_id,
                self._torrent.info_hash,
                self._active_peers_available,
            )
            for _ in range(self.MAX_CONNECTIONS)
        ]

    async def run(self):
        # we do initial handshake with peers and then we run it as a background job
        await self._handshake_peers()
        self._peer_refresh_task = asyncio.create_task(self._refresh_peers_loop())
        await asyncio.gather(*(worker.run() for worker in self._workers))

    async def _load_peers(self) -> None:
        """
        Send UDP announce request and get information about the current peers
        """
        peer_sets = await asyncio.gather(
            *(self._announce_request(x) for x in self._torrent.announce_list),
        )
        self._peers = set(itertools.chain.from_iterable(peer_sets))

        # remove peers that we cannot create connection with

    async def _announce_request(self, announce) -> Set['Peer']:
        """
        Helper method for sending udp announce requests, if there is any exception we
        return just empty set, otherwise we initialize Peer objects

        This is mostly wrapper so we can call asyncio.gather around it
        """
        tracker = udp_tracker.UDPTracker(
            announce.hostname, announce.port, self._peer_id, self._torrent
        )

        peers = set()
        try:
            peer_info = await tracker.get_peer_set()
        except:
            return peers

        for ip, port in peer_info:
            peers.add(
                Peer(
                    ip=ip,
                    port=port,
                    peer_id=self._peer_id,
                    file_queue=self._file_queue,
                    message_handler=self._message_handler,
                )
            )

        return peers

    async def _refresh_peers_loop(self):
        """
        Downloads peer list and tries to handshake them every `REFRESH_PEERS_DELAY` seconds
        """
        while True:
            await asyncio.sleep(self.REFRESH_PEERS_DELAY)
            self._logger.info('Running refresh peers loop')
            await self._load_peers()

            # we don't want to handshake also peers that we have already handshaked
            # and are now in session with
            peers_to_be_removed = set()
            for peer in self._peers:
                if peer in self._active_peers:
                    logging.debug('Removing already handshaked peer = %s', peer)
                    peers_to_be_removed.add(peer)
            self._peers -= peers_to_be_removed

            await self._handshake_peers()

    async def _handshake_peers(self) -> None:
        """"""
        # first we need to establish connection

        await asyncio.gather(*(peer.create_connection() for peer in self._peers))
        self._peers = set(peer for peer in self._peers if peer.is_opened)

        await asyncio.gather(
            *(
                peer.make_handshake(self._peer_id, self._torrent.info_hash)
                for peer in self._peers
            )
        )

        self._active_peers |= set(peer for peer in self._peers if peer.handshaked)
        if not self._active_peers_available.is_set() and self._active_peers:
            self._active_peers_available.set()


class PeerWorker:
    def __init__(self, peers: Set['Peer'], handshaked_peers, peer_id, info_hash, test):
        self.peers = peers
        self._id = uuid.uuid4()
        self._peer: Optional[Peer] = None
        self._info_hash = info_hash
        self._peer_id = peer_id
        self._handshaked_peers: Set[Peer] = handshaked_peers
        self._active_peers_available: asyncio.Event = test
        self._logger = logging.getLogger(self.__class__.__name__)

    async def run(self):
        while True:
            if self._peer is None:
                self._peer = await self._get_peer()

            if self._peer.choked:
                if await self._peer.send_interested() is True:
                    self._peer = None
                    continue

            await self._run_piece_request_loop()

    async def _run_piece_request_loop(self):
        retry_count = 0
        while True:
            try:
                await self._peer.download_piece()
            except PeerResponseError:
                retry_count += 1
                self._logger.debug(
                    'Peer %s sent empty response, retry count: %d', self, retry_count
                )
                if retry_count == 3:
                    self._peer = None
                    break
            await asyncio.sleep(0)

    async def _get_peer(self):
        while True:
            try:
                await asyncio.wait_for(self._active_peers_available.wait(), None)
                peer = self._handshaked_peers.pop()
                self._logger.info(f'Worker acquired peer = {peer}')
            except KeyError:
                self._active_peers_available.clear()
                # await asyncio.sleep(10)
            else:
                peer.is_available = False
                return peer


class Peer:
    """
    Peer class implementation for communicating
    """

    MINIMUM_HANDSHAKE_DELAY = 130

    def __init__(self, ip, port, peer_id, file_queue, message_handler):
        self._id = uuid.uuid1()
        self._ip = ip
        self._peer_id = peer_id
        self._port = port
        self._message_handler = message_handler
        self._bitfield: Optional[message.BitField] = None
        self._writer = None
        self._reader = None
        self.handshaked = False
        self.choked = True
        self.is_available = True
        self._piece_manager = file_queue
        self._protocol = protocols.PeerProtocol(
            peer_id=self._peer_id,
            ip=self._ip,
            port=self._port,
        )
        self._current_piece: Optional[piece.Piece] = None
        self.last_handshake = None

    def __str__(self):
        return f'Peer[{self._id}] - <{self._ip}:{self._port}>'

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(f'{self._ip}{self._port}')

    @property
    def is_opened(self):
        return self._protocol.is_opened()

    async def create_connection(self) -> None:
        try:
            await self._protocol.connect()
        except protocols.PeerUnavailableError:
            return None

    async def make_handshake(self, peer_id: bytes, info_hash: bytes) -> None:

        handshake = message.Handshake.to_bytes(peer_id, info_hash)

        response = await self._protocol.send_message(handshake)
        messages = self._message_handler.process_handshake(response)

        if messages is not None:
            self.handshaked = True
            try:
                self._bitfield = messages[1]
            except IndexError:
                pass

    def has_piece(self, index):
        return self._bitfield.bitfield[index]

    async def send_interested(self) -> bool:
        response = await self._protocol.send_message(message.Interested.to_bytes())

        # we don't really care for any other response than Unchoked
        if isinstance(self._message_handler.process_message(response), message.Unchoke):
            self.choked = False

        return self.choked

    async def download_piece(self):
        if not self._current_piece:
            self._current_piece = self._piece_manager.get_available_piece(
                self._bitfield.bitfield
            )

        current_block_number = 0
        while current_block_number < len(self._current_piece.blocks):
            block = self._current_piece.blocks[current_block_number]
            if block.data:
                # skip already downloaded blocks
                print('skipping block')
                current_block_number += 1
                continue

            request_message = message.Request().to_bytes(
                self._current_piece.index, block.offset
            )
            response = await self._protocol.send_message(request_message)

            if not response:
                self._current_piece.queued = False
                self._current_piece = None
                raise PeerResponseError

            if isinstance(
                (piece_message := self._message_handler.process_message(response)),
                message.Piece,
            ):
                print(
                    f'received block number {current_block_number}/{len(self._current_piece.blocks)} for piece {self._current_piece.index}'
                )
                block.data = piece_message.block_data
                current_block_number += 1
            else:
                # TODO: maybe add some counter that checks how many times has peer send
                # TODO: incorrect data so it won't be inifinite cycle?
                pass

        else:
            # TODO: we should probably drop peer if he is sending non valid pieces?
            self._current_piece.validate()
            self._current_piece = None


class PeerResponseError(Exception):
    pass
