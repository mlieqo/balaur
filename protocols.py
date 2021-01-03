from typing import Callable, Any, Optional

import asyncio
import logging


logger = logging.getLogger(__name__)


# def reconnect(func: Callable[..., Any]):
#     async def wrapper(self, *args, **kwargs):
#         try:
#             return await func(self, *args, **kwargs)
#         except (ConnectionResetError, AttributeError):
#             try:
#                 await self._connect()
#             except ConnectionRefusedError:
#                 print('inside connection refused')
#                 return None
#             return await func(self, *args, **kwargs)
#     return wrapper


class PeerProtocol:
    def __init__(self, peer_id, ip, port):
        self._peer_id = peer_id
        self._ip = ip
        self._port = port
        self._writer: Optional[asyncio.StreamWriter] = None
        self._reader: Optional[asyncio.StreamReader] = None

    async def _connect(self):
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self._ip, self._port), timeout=3
            )
        except asyncio.TimeoutError:
            pass

    async def send_message(self, message):
        try:
            self._writer.write(message)
            await self._writer.drain()
            return await self._read_response()
        except ConnectionRefusedError as e:
            logger.error('Cannot write mesage = %s', e)

    async def _read_response(self):
        whole_response = b''
        while True:
            try:
                response = await asyncio.wait_for(self._reader.read(4096), timeout=1.5)
            except asyncio.TimeoutError:
                break
            else:
                if not response:
                    break
                whole_response += response
        return whole_response
