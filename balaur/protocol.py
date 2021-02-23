from typing import Optional

import asyncio

import logwood


class PeerUnavailableError(Exception):
    pass


class PeerProtocol:
    def __init__(self, peer_id, ip, port):
        self._peer_id = peer_id
        self._ip = ip
        self._port = port
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader: Optional[asyncio.StreamReader] = None
        self._logger = logwood.get_logger(self.__class__.__name__)

    def is_opened(self):
        return self._writer is not None and self._reader is not None

    async def connect(self):
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._ip, self._port), timeout=3
            )
        except (asyncio.TimeoutError, ConnectionRefusedError, OSError) as exc:
            raise PeerUnavailableError from exc

    async def send_message(self, message):
        try:
            self._writer.write(message)
            await self._writer.drain()
        except (ConnectionRefusedError, ConnectionResetError) as e:
            self._logger.error('Cannot write mesage = %s', e)
            return None
        else:
            return await self._read_response()

    async def _read_response(self):
        whole_response = b''
        while True:
            try:
                response = await asyncio.wait_for(self._reader.read(4096), timeout=1)
            except (asyncio.TimeoutError, ConnectionResetError):
                break
            else:
                if not response:
                    break
                whole_response += response
                # if len(whole_response) == 16397 or len(whole_response) == 10253:
                #     break

        return whole_response
