from typing import List, Optional, Set

import asyncio
import bitstring
import itertools
import uuid

import logwood

import message
import torrent
import udp_tracker
import protocol
import piece


class PeerManager:
    MAX_CONNECTIONS = 50
    REFRESH_PEERS_DELAY = 120

    def __init__(self, torrent: torrent.Torrent, peer_id: bytes, file_queue):
        self._peers: Optional[Set['Peer']] = None
        self._active_peers = set()
        self._torrent = torrent
        self._peer_id = peer_id
        self._file_queue = file_queue
        self._message_handler = message.MessageHandler()
        self._trackers: Optional[List[udp_tracker.UDPTracker]] = None
        self._workers: Optional[List[PeerWorker]] = None
        self._active_peers_available: Optional[asyncio.Event] = None
        self._peer_refresh_task: Optional[asyncio.Task] = None
        self._logger = logwood.get_logger(self.__class__.__name__)

    async def start(self) -> None:
        """
        Loads `self._trackers`, `self._peers` and initializes PeerWorker `self._workers`
        based on `MAX_CONNECTIONS` amount.
        """

        # initialize event here, because `EventLoop` might have not been started yet during initialization
        self._active_peers_available = asyncio.Event()

        # TODO: currently only udp trackers
        self._trackers = [
            udp_tracker.UDPTracker(
                ip=announce.hostname,
                port=announce.port,
                peer_id=self._peer_id,
                torrent=self._torrent,
            )
            for announce in self._torrent.announce_list
            if announce.scheme == 'udp'
        ]

        await self._load_peers()

        self._workers = [
            PeerWorker(
                id=i,
                active_peers=self._active_peers,
                peer_id=self._peer_id,
                info_hash=self._torrent.info_hash,
                active_peers_available=self._active_peers_available,
            )
            for i in range(self.MAX_CONNECTIONS)
        ]

    async def run(self) -> None:
        """
        Does initial handshake with `self._peers`, then schedules `self._refresh_peers_loop` to run
        every `REFRESH_PEERS_DELAY` and runs all workers in background
        """
        await self._handshake_peers()
        self._peer_refresh_task = asyncio.create_task(self._refresh_peers_loop())
        await asyncio.gather(*(worker.run() for worker in self._workers))

    async def _load_peers(self) -> None:
        """
        Send UDP announce request and get information about the current peers
        """
        peer_sets = await asyncio.gather(
            *(self._get_peers(tracker) for tracker in self._trackers),
        )
        self._peers = set(itertools.chain.from_iterable(peer_sets))

    async def _get_peers(self, tracker: udp_tracker.UDPTracker) -> Set['Peer']:
        """
        Helper method for obtaining peers from tracker, used as a wrapper around
        `tracker.get_peers` method for handling exceptions and creating a `Peer` objects
        out of addresses.
        """
        peers = set()

        try:
            peer_addresses = await tracker.get_peers()
        except Exception as e:
            self._logger.error('Error loading peers from tracker = %s: %s', tracker, e)
        else:
            for ip, port in peer_addresses:
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

    async def _refresh_peers_loop(self) -> None:
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
                    self._logger.debug('Removing already handshaked peer = %s', peer)
                    peers_to_be_removed.add(peer)
            self._peers -= peers_to_be_removed

            await self._handshake_peers()

    async def _handshake_peers(self) -> None:
        """
        Creates TCP connection with peer, sends handshake and stores successfuly handshaked
        peers in `self._active_peers`
        """

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
            self._logger.debug('Setting event active_peers_available')
            self._active_peers_available.set()


class PeerWorker:
    """
    Worker for handling the peer communication flow.
    """

    # number of retries for downloading piece after invalid peer response
    PIECE_RETRY_COUNT = 3

    def __init__(
        self,
        id: int,
        active_peers: Set['Peer'],
        peer_id: bytes,
        info_hash: bytes,
        active_peers_available: asyncio.Event,
    ):
        self._id = id
        self._peer: Optional[Peer] = None
        self._info_hash = info_hash
        self._peer_id = peer_id
        self._active_peers = active_peers
        self._active_peers_available: asyncio.Event = active_peers_available
        self._logger = logwood.get_logger(self.__class__.__name__)

    async def run(self) -> None:
        """
        Flow: get peer -> unchoke (send interested) -> request piece (loop)
        """
        while True:
            if self._peer is None:
                await self._get_peer()

            # every peer starts as `choked`
            if self._peer.choked:
                await self._peer.send_interested()
                if self._peer.choked:
                    self._logger.info(
                        'Worker[%s] - peer %s not interested', self._id, self._peer
                    )
                    self._peer = None
                    continue

            await self._run_piece_request_loop()

    async def _run_piece_request_loop(self) -> None:
        """
        Runs a piece request in a loop. Worker drops peer if he sends invalid response
        `PIECE_RETRY_COUNT` times.
        """

        retry_count = 0

        while True:
            try:
                await self._peer.download_piece()
            except PeerResponseError:
                if retry_count == self.PIECE_RETRY_COUNT:
                    self._peer = None
                    break

                retry_count += 1
                self._logger.debug(
                    'Peer %s sent empty response, retrying for %d. time',
                    self,
                    retry_count,
                )
            await asyncio.sleep(0)

    async def _get_peer(self) -> None:
        """
        Get peer if there is any, if not then unset the `self._active_peers_available` event, that
        is shared between all other `PeerWorker` instances and wait for new active peers.
        """
        while True:
            try:
                await asyncio.wait_for(self._active_peers_available.wait(), None)
                # throws KeyError if there's no element
                peer = self._active_peers.pop()
            except KeyError:
                self._active_peers_available.clear()
                self._logger.debug('No active peers available')
            else:
                self._peer = peer
                self._logger.info('Worker[%s] acquired peer = %s', self._id, peer)
                return


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
        self._bitfield: Optional[bitstring.BitArray] = None
        self.handshaked = False
        self.choked = True
        self._piece_manager: piece.PieceManager = file_queue
        self._protocol = protocol.PeerProtocol(
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
        except protocol.PeerUnavailableError:
            return None

    async def make_handshake(self, peer_id: bytes, info_hash: bytes) -> None:

        handshake = message.Handshake.to_bytes(peer_id, info_hash)

        response = await self._protocol.send_message(handshake)
        messages = self._message_handler.process_handshake(response)

        if messages is not None:
            self.handshaked = True
            try:
                self._bitfield = messages[1].bitfield
            except IndexError:
                # in case peer has not send bitfield, we just assume he has all the pieces
                self._bitfield = bitstring.BitArray(
                    length=self._piece_manager.number_of_pieces
                )

    async def send_interested(self) -> bool:
        response = await self._protocol.send_message(message.Interested.to_bytes())

        # we don't really care for any other response than Unchoked
        if isinstance(self._message_handler.process_message(response), message.Unchoke):
            self.choked = False

        return self.choked

    async def download_piece(self):
        if not self._current_piece:
            self._current_piece = self._piece_manager.get_available_piece(
                self._bitfield
            )

        while self._current_piece:
            request_message = message.Request().to_bytes(
                self._current_piece.index,
                self._current_piece.current_block_offset,
                self._current_piece.current_block_length,
            )
            response = await self._protocol.send_message(request_message)
            if not response:
                # let piece manager know that piece is not being downloaded anymore
                self._piece_manager.return_piece_by_index(self._current_piece.index)
                self._current_piece = None
                raise PeerResponseError

            if isinstance(
                (piece_message := self._message_handler.process_message(response)),
                message.Piece,
            ):
                if self._current_piece.add_block_data(piece_message.block_data) is True:
                    self._current_piece = None


class PeerResponseError(Exception):
    """Empty or invalid response"""
