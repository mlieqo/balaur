from typing import Optional, Set, Tuple
import asyncio
import random
import socket
import struct

import logwood

import balaur.torrent


logger = logwood.get_logger(__name__)


class UDPTracker:
    """
    Tracker class for sending connection and announce request and establishing
    connection with UDP tracker. After successful connection, information about
    peers is exchanged.
    """

    REQUEST_TIMEOUT = 5  # seconds

    def __init__(
        self, ip: str, port: int, peer_id: bytes, torrent: balaur.torrent.Torrent
    ):
        self._address = (ip, port)
        self._peer_id = peer_id
        self._info_hash = torrent.info_hash
        self._torrent_size = torrent.torrent_size
        self._tracker_connection = UDPTrackerConnection()
        self._tracker_announce: Optional[UDPTrackerAnnounce] = None
        self._peers: Optional[Set[Tuple[str, int]]] = None
        self._connection_id: Optional[int] = None
        self._protocol: Optional[UDPClientProtocol] = None
        self._on_connection_lost = asyncio.Event()

    def __str__(self):
        return f'{self.__class__.__name__}[{self._address}]'

    async def _create_connection(self) -> None:
        loop = asyncio.get_running_loop()
        self._on_connection_lost.clear()
        _, self._protocol = await loop.create_datagram_endpoint(
            lambda: UDPClientProtocol(self._on_connection_lost),
            remote_addr=self._address,
        )

    async def get_peers(self) -> Set[Tuple[str, int]]:
        """
        First send the connection request, if the connection_id received back is OK
        we can continue with sending announce request
        """
        if self._protocol is None or self._on_connection_lost.is_set():
            await self._create_connection()

        if self._connection_id is None:
            await self._send_connection_request()

        await self._send_announce_request()
        return self._peers

    async def _send_connection_request(self) -> None:
        self._protocol.send_message(self._tracker_connection.message)
        buffer = await asyncio.wait_for(
            self._protocol.buffer.get(), timeout=self.REQUEST_TIMEOUT
        )
        self._connection_id = self._tracker_connection.read(buffer)

    async def _send_announce_request(self) -> None:
        if self._tracker_announce is None:
            self._tracker_announce = UDPTrackerAnnounce(
                connection_id=self._connection_id,
                info_hash=self._info_hash,
                torrent_size=self._torrent_size,
                peer_id=self._peer_id,
            )
        self._protocol.send_message(self._tracker_announce.message)
        # TODO: problem if response comes in multiple datagrams?
        buffer = await asyncio.wait_for(
            self._protocol.buffer.get(), timeout=self.REQUEST_TIMEOUT
        )
        self._peers = self._tracker_announce.read(buffer)


class UDPClientProtocol(asyncio.DatagramProtocol):
    """
    UDP protocol that connects to the address. Individual messages can be send using `send_message` method,
    and response is offered through `self.buffer` queue. If connection is lost, it sets `on_connection_lost`
    event.
    """

    def __init__(self, on_connection_lost: asyncio.Event):
        self._on_connection_lost = on_connection_lost
        self._transport: Optional[asyncio.DatagramTransport] = None
        self.buffer: asyncio.Queue[bytes] = asyncio.Queue()

    def send_message(self, message: bytes) -> None:
        self._transport.sendto(message)

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self._transport = transport

    def datagram_received(self, data, addr) -> None:
        logger.debug('<%s> Received from %s: %s', self.__class__.__name__, addr, data)
        self.buffer.put_nowait(data)

    def error_received(self, exc) -> None:
        logger.error('<%s> Error received: %s', self.__class__.__name__, exc)

    def connection_lost(self, exc) -> None:
        logger.debug('<%s> Connection closed..', self.__class__.__name__)
        self._on_connection_lost.set()


class UDPTrackerConnection:
    def __init__(self):
        self._transaction_id: int = random.randrange(0, 10000)
        # we store it on init as the message remains the same during whole lifecycle
        self.message: bytes = self._build_message()

    def _build_message(self) -> bytes:
        connection_id = struct.pack('!Q', 0x41727101980)
        action = struct.pack("!I", 0)
        transaction_id = struct.pack("!I", self._transaction_id)
        buffer = connection_id + action + transaction_id
        return buffer

    def read(self, buffer: bytes) -> Optional[int]:
        (action,) = struct.unpack_from("!I", buffer)
        (res_transaction_id,) = struct.unpack_from("!I", buffer, 4)
        if res_transaction_id != self._transaction_id:
            logger.error('Incorrect transaction ID in UDPTrackerConnection response')
            return
        if action == 0:
            (connection_id,) = struct.unpack_from("!q", buffer, 8)
            return connection_id


class UDPTrackerAnnounce:
    def __init__(
        self, connection_id: int, info_hash: bytes, peer_id: bytes, torrent_size: int
    ):
        self._transaction_id = random.randrange(0, 10000)
        self.message = self._build_message(
            connection_id, info_hash, peer_id, torrent_size
        )

    def _build_message(
        self, connection_id: int, info_hash: bytes, peer_id: bytes, torrent_size: int
    ) -> bytes:
        connection_id = struct.pack('!q', connection_id)
        action = struct.pack('!I', 1)
        transaction_id = struct.pack('!I', self._transaction_id)
        downloaded = struct.pack('!Q', 0)
        t_size = struct.pack('!Q', torrent_size)
        uploaded = struct.pack('!Q', 0)
        event = struct.pack('!I', 2)
        ip = struct.pack('!I', 0)
        key = struct.pack('!I', random.randrange(0, 255))
        num_peers = struct.pack('!i', -1)
        port = struct.pack('!H', 6889)
        return (
            connection_id
            + action
            + transaction_id
            + info_hash
            + peer_id
            + downloaded
            + t_size
            + uploaded
            + event
            + ip
            + key
            + num_peers
            + port
        )

    @staticmethod
    def read(buffer: bytes) -> Set[Tuple[str, int]]:
        (action,) = struct.unpack(">I", buffer[0:4])  # first 4 bytes is action

        if action == 1:
            # ret = dict()
            # ret['action'] = action
            # (res_transaction_id,) = struct.unpack(
            #     "!I", buffer[4:8]
            # )  # next 4 bytes is transaction id
            # ret['transaction_id'] = res_transaction_id
            # (ret['interval'],) = struct.unpack("!I", buffer[8:12])
            # (ret['leeches'],) = struct.unpack("!I", buffer[12:16])
            # (ret['seeds'],) = struct.unpack("!I", buffer[16:20])
            peers = set()
            # first 20 bytes are action/transaction_id/interval etc..
            offset = 20
            while offset != len(buffer):
                ip = struct.unpack_from("!I", buffer, offset)[0]
                ip = socket.inet_ntoa(struct.pack('!I', ip))
                offset += 4
                port = struct.unpack_from("!H", buffer, offset)[0]
                peers.add((ip, port))
                offset += 2
            return peers
