from typing import Optional, List
import asyncio

import logwood

import balaur.bittorrent_message


class PeerUnavailableError(Exception):
    """ Unable to connect or send bytes to peer """


class PeerProtocol:
    """
    Protocol class for communicating with peers. Received raw data
    is processed and parsed into `bittorrent_message`.
    """

    RESPONSE_TIMEOUT = 1.5
    READ_BUFFER = 4096

    def __init__(self, peer_id, ip, port, parser):
        self._peer_id = peer_id
        self._ip = ip
        self._port = port
        self._parser = parser
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._logger = logwood.get_logger(self.__class__.__name__)

    @property
    def is_opened(self) -> bool:
        return self._writer is not None and self._reader is not None

    async def connect(self) -> None:
        """
        Establish network connection with `self._ip` and `self._port` address
        """
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._ip, self._port), timeout=3
            )
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as exc:
            raise PeerUnavailableError from exc

    async def _send_bytes(self, data: bytes) -> None:
        """
        Sends `data` bytes through the established network connection

        Raises `PeerUnavailableError` if the connection is broken.
        """
        try:
            self._writer.write(data)
            await self._writer.drain()
        except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError) as e:
            self._logger.error('Cannot write message = %s', e)
            raise PeerUnavailableError from e

    async def send_handshake(self, handshake: bytes) -> Optional[List]:
        """
        Sends handshake message bytes and then immediately read and parse response.
        """
        try:
            await self._send_bytes(handshake)
        except PeerUnavailableError:
            return None
        else:
            data = b''
            while True:
                try:
                    response = await asyncio.wait_for(
                        self._reader.read(self.READ_BUFFER),
                        timeout=self.RESPONSE_TIMEOUT,
                    )
                except (asyncio.TimeoutError, ConnectionResetError):
                    break
                else:
                    if not response:
                        break
                    data += response
            return self._parser.process_handshake(data)

    async def send_message(
        self, message: bytes
    ) -> Optional[balaur.bittorrent_message.BaseMessage]:
        """
        Sends message, then reads and parses the response.
        """
        try:
            await self._send_bytes(message)
        except PeerUnavailableError:
            return None
        else:
            return await self._read_and_parse_response()

    async def _read_and_parse_response(
        self,
    ) -> Optional[balaur.bittorrent_message.BaseMessage]:
        """
        Reads response until whole message has been received and then tries to parse it.
        The length of message is indicated by the first 4 bytes of the message.
        """
        data = b''
        while True:
            try:
                response = await asyncio.wait_for(
                    self._reader.read(self.READ_BUFFER), timeout=self.RESPONSE_TIMEOUT
                )
            except (asyncio.TimeoutError, ConnectionResetError):
                return None
            else:
                if not response:
                    return None
                if not data:
                    # it's first message we received from peer, we read message length
                    # so we know when we received the full message
                    if len(response) < 4:
                        return None

                    message_length = (
                        self._parser.get_length(response) + 4
                    )  # 4 bytes are the length information
                data += response
                if len(data) >= message_length:
                    return self._parser.process_message(data[:message_length])
