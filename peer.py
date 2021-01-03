from typing import List, Optional, Set

import asyncio
import uuid
import time
import itertools

import message
import torrent
import udp_tracker
import protocols
import piece


class PeerManager:
    """
    missing
    """

    MAX_CONNECTIONS = 20
    HANDSHAKE_DELAY = 120

    def __init__(self, torrent_: torrent.Torrent, peer_id: bytes, file_queue):
        self._peer_set: Optional[List[List['Peer']]] = None
        self._torrent = torrent_
        self._peer_id = peer_id
        self._workers = None
        self._file_queue = file_queue
        self._handshaked_peers = set()

    async def start(self):
        self._peer_set = await self._get_peer_set()
        self._workers = [
            PeerWorker(
                self._peer_set,
                self._handshaked_peers,
                self._peer_id,
                self._torrent.info_hash
            )
            for _ in range(self.MAX_CONNECTIONS)
        ]

    async def run(self):
        # we do initial handshake with peers and then we run it as a background job
        print(f'peer set before handshake {self._peer_set}')
        await self._handshake_peers()
        asyncio.create_task(self._handshake_peers_loop())
        await asyncio.gather(*(worker.run() for worker in self._workers))

    async def _get_peer_set(self):
        peer_sets = await asyncio.gather(
            *(self._announce_request(x) for x in self._torrent.announce_list),
            # we return also exceptions as sometimes announce response comes with
            # negative connection id that causes exception 'int too large to convert'
            # when building announce request
            # return_exceptions=True
        )
        return set(itertools.chain.from_iterable(peer_sets))

    async def _announce_request(self, announce):
        _udp_tracker = udp_tracker.UDPTracker(
            announce.hostname,
            announce.port,
            self._peer_id,
            self._torrent
        )
        peer_object_set = set()

        try:
            peers = await _udp_tracker.get_peer_set()
        except:
            return peer_object_set

        for ip, port in peers:
            peer_object_set.add(Peer(ip=ip, port=port, peer_id=self._peer_id, file_queue=self._file_queue))
        return peer_object_set

    async def _handshake_peers_loop(self):
        while True:
            await asyncio.sleep(self.HANDSHAKE_DELAY)
            print('running handshake peers from background')
            await self._handshake_peers()

    async def _handshake_peers(self):
        """
        Handshake all peers only at the beginning before download starts, then handshakes are
        done by peer worker.
        """
        handshaked_peers = await asyncio.gather(
            *(
                peer.make_handshake(self._peer_id, self._torrent.info_hash)
                for peer in self._peer_set
            ),
            return_exceptions=True
        )
        print(f'handshaked peers after handshake {handshaked_peers}')
        self._handshaked_peers |= set(peer for peer in handshaked_peers if isinstance(peer, Peer))
        self._peer_set -= self._handshaked_peers
        print(self._handshaked_peers)


class PeerWorker:
    def __init__(self, peers: Set['Peer'], handshaked_peers, peer_id, info_hash):
        self.peers = peers
        self._peer: Optional[Peer] = None
        self._info_hash = info_hash
        self._peer_id = peer_id
        self._handshaked_peers: Set[Peer] = handshaked_peers

    async def run(self):
        while True:
            print(' i am here ')
            if self._peer is None:
                print('peer is none')
                self._peer = await self._get_peer()

            if self._peer.choked:
                if await self._peer.send_interested() is True:
                    self._peer = None
                    continue

            await self._run_piece_request_loop()

    async def _run_piece_request_loop(self):
        while True:
            try:
                await self._peer.download_piece()
            except PeerResponseError:
                print('bitch send me empty response')
                self._peer = None
                break
            await asyncio.sleep(0)

    async def _get_peer(self):
        while True:
            try:
                # TODO: there is also chance that the handshake is old
                peer = self._handshaked_peers.pop()
            except KeyError:
                await asyncio.sleep(10)
            else:
                peer.is_available = False
                return peer


class Peer:
    """
    Peer class implementation for communicating
    """

    MINIMUM_HANDSHAKE_DELAY = 130

    def __init__(self, ip, port, peer_id, file_queue):
        self._id = uuid.uuid1()
        self._ip = ip
        self._peer_id = peer_id
        self._port = port
        self._bitfield = None
        self._writer = None
        self._reader = None
        self._handshaked = False
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

    def __hash__(self):
        return hash(f'{self._ip}{self._port}')

    async def make_handshake(self, peer_id: bytes, info_hash: bytes):
        handshake = message.Handshake(peer_id, info_hash)
        self.last_handshake = time.time()
        print('sending handshake')

        response = await self._protocol.send_message(handshake.to_bytes())
        processed_message = message.process_handshake_response(response)

        if processed_message is not None and isinstance(processed_message[0], message.Handshake):
            self._handshaked = True
            print('handshaked')
            if len(processed_message) > 1 and isinstance(processed_message[1], message.BitField):
                self._bitfield = processed_message[1].bitfield
            return self

        return None

    @property
    def can_be_handshaked(self):
        return time.time() - self.last_handshake > self.MINIMUM_HANDSHAKE_DELAY

    @property
    def has_handshaked(self):
        return self._handshaked

    def has_piece(self, index):
        return self._bitfield[index]

    async def send_interested(self) -> bool:
        response = await self._protocol.send_message(
            message.Interested().to_bytes()
        )

        # we don't really care for any other response than Unchoked
        if isinstance(message.process_message(response), message.Unchoke):
            self.choked = False
            print('Unchoked')
        return self.choked

    async def download_piece(self):
        if not self._current_piece:
            self._current_piece = self._piece_manager.get_available_piece(self._bitfield)

        current_block_number = 0
        while current_block_number < len(self._current_piece.blocks):
            block = self._current_piece.blocks[current_block_number]
            if block.data:
                # skip already downloaded blocks
                print('skipping block')
                current_block_number += 1
                continue

            request_message = message.Request().to_bytes(self._current_piece.index, block.offset)
            response = await self._protocol.send_message(request_message)

            if not response:
                self._current_piece.queued = False
                raise PeerResponseError

            if isinstance((piece_message := message.process_message(response)), message.Piece):
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


class NoFreeBlocksError(Exception):
    pass


class PeerResponseError(Exception):
    pass
