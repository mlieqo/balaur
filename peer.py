from typing import List, Optional

import asyncio
import uuid
import time

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

    def __init__(self, torrent_: torrent.Torrent, peer_id: bytes, file_queue):
        self._peers_lists: Optional[List[List['Peer']]] = None
        self._torrent = torrent_
        self._peer_id = peer_id
        self._workers = None
        self._file_queue = file_queue

    async def start(self):
        self._peers_lists = await self._get_peer_lists()
        self._workers = [PeerWorker(self._peers_lists[0], self._peer_id, self._torrent.info_hash)
                         for _ in range(self.MAX_CONNECTIONS)]

    async def run(self):
        # spawn MAX_CONNECTIONS amount of workers that will have each their own peer
        # and will take care of the whole lifecycle
        # for i in range(self.MAX_CONNECTIONS):
        await self.handshake_peers()
        # await self.send_interested()
        await asyncio.gather(*(worker.run() for worker in self._workers))

    async def _get_peer_lists(self):
        peer_lists = await asyncio.gather(
            *(self._announce_request(x) for x in self._torrent.announce_list),
            # we return also exceptions as sometimes announce response comes with
            # negative connection id that causes exception 'int too large to convert'
            # when building announce request
            return_exceptions=True
        )
        return [x for x in peer_lists if isinstance(x, list)]

    async def _announce_request(self, announce):
        _udp_tracker = udp_tracker.UDPTracker(
            announce.hostname,
            announce.port,
            self._peer_id,
            self._torrent
        )
        try:
            peers = await _udp_tracker.get_peer_list()
        except asyncio.TimeoutError:
            return None
        peer_list = []
        for ip, port in peers:
            peer_list.append(Peer(ip=ip, port=port, peer_id=self._peer_id, file_queue=self._file_queue))
        return peer_list

    async def handshake_peers(self):
        """
        Handshake all peers only at the beginning before download starts, then handshakes are
        done by peer worker.
        """
        await asyncio.gather(
            *(
                peer.make_handshake(self._peer_id, self._torrent.info_hash)
                for peer in self._peers_lists[0]
            ),
            return_exceptions=True
        )


class PeerWorker:
    def __init__(self, peers: List['Peer'], peer_id, info_hash):
        self.peers = peers
        self._peer: Optional[Peer] = None
        self._info_hash = info_hash
        self._peer_id = peer_id

    async def run(self):
        while True:
            print(' i am here ')
            if self._peer is None:
                print('peer is none')
                self._peer = await self._get_peer()

            if self._peer.choked:
                if await self._peer.send_interested() is False:
                    await self._run_piece_request_loop()
                else:
                    self._peer = None

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
        for peer in self.peers:
            if peer.is_available and peer.has_handshaked:
                # self._peer = peer
                peer.is_available = False
                return peer
            # we wait until maybe one of the peers will be available
            # TODO: maybe there is peer available but has not handshaked so we should try to handshake here again
        print('inside get_peer loop')

        while True:
            for peer in self.peers:
                if peer.is_available and not peer.has_handshaked:
                    if peer.can_be_handshaked and await peer.make_handshake(self._peer_id, self._info_hash):
                        peer.is_available = False
                        self._peer = peer

            await asyncio.sleep(Peer.MINIMUM_HANDSHAKE_DELAY)


class Peer:
    """
    Peer class implementation for communicating
    """

    MINIMUM_HANDSHAKE_DELAY = 60

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

    async def make_handshake(self, peer_id: bytes, info_hash: bytes):
        handshake = message.Handshake(peer_id, info_hash)
        self.last_handshake = time.time()
        print('sending handshake')

        response = await self._protocol.send_message(handshake.to_bytes())
        processed_message = message.process_handshake_response(response)

        if isinstance(processed_message[0], message.Handshake):
            self._handshaked = True
            print('handshaked')
        if len(processed_message) > 1 and isinstance(processed_message[1], message.BitField):
            self._bitfield = processed_message[1].bitfield

        return self._handshaked

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

        for block in self._current_piece.blocks:
            if block.data:
                # skip already downloaded blocks
                continue

            request_message = message.Request().to_bytes(self._current_piece.index, block.offset)
            response = await self._protocol.send_message(request_message)

            if not response:
                self._current_piece.queued = False
                raise PeerResponseError

            if isinstance((piece_message := message.process_message(response)), message.Piece):
                block.data = piece_message.block_data
            else:
                # TODO: something should happend here if it's not piece :(
                pass
        else:
            # TODO: we should probably drop peer if he is sending non valid pieces?
            self._current_piece.validate()
            self._current_piece = None


class NoFreeBlocksError(Exception):
    pass


class PeerResponseError(Exception):
    pass
