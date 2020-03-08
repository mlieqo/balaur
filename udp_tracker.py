import asyncio
from logging import getLogger
from message import UDPTrackerConnection, UDPTrackerAnnounce
from torrent import Torrent

logger = getLogger(__name__)


class UDPClientProtocol(asyncio.BaseProtocol):
    def __init__(self, message, on_con_lost):
        self.message = message
        self.on_con_lost = on_con_lost
        self.transport = None
        self.data = None

    def connection_made(self, transport):
        self.transport = transport
        logger.debug('<%s> Sent: %s', self.__class__.__name__, self.message)
        self.transport.sendto(self.message)

    def datagram_received(self, data, addr):
        logger.debug('<%s> Received: %s', self.__class__.__name__, data)
        self.data = data
        self.transport.close()

    def error_received(self, exc):
        logger.error('<%s> Error received: %s', self.__class__.__name__, exc)

    def connection_lost(self, exc):
        logger.debug('<%s> Connection closed..', self.__class__.__name__)
        self.on_con_lost.set_result(True)


class UDPTracker:
    def __init__(self, ip, port, torrent: Torrent):
        self.peer_list = None
        self.connection_id = None
        self.conn = (ip, port)
        self.peer_id = torrent.peer_id
        self.info_hash = torrent.info_hash
        self.torrent_size = torrent.torrent_size
        self.udp_conn = UDPTrackerConnection()
        self.udp_announce = None

    async def get_peer_list(self):
        await self._send_conn_req()
        await self._send_announce_req()
        return self.peer_list

    async def _send_conn_req(self):
        buff = await self._udp_send_message(
            self.udp_conn.build_msg(),
            self.conn
        )
        self.connection_id = self.udp_conn.read(buff)
        return self.connection_id

    async def _send_announce_req(self):
        if self.udp_announce is None:
            self.udp_announce = UDPTrackerAnnounce(
                connection_id=self.connection_id,
                info_hash=self.info_hash,
                torrent_size=self.torrent_size,
                peer_id=self.peer_id
            )
        buff = await self._udp_send_message(
            self.udp_announce.build_msg(),
            self.conn
        )
        ret, self.peer_list = self.udp_announce.read(buff)
        return self.peer_list

    @staticmethod
    async def _udp_send_message(msg, addr, timeout=0.5):
        loop = asyncio.get_running_loop()
        on_con_lost = loop.create_future()
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPClientProtocol(msg, on_con_lost),
            remote_addr=addr
        )
        await asyncio.wait_for(on_con_lost, timeout)
        return protocol.data

