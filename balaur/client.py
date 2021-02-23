import asyncio
import struct

import logwood

logwood.basic_config(level=logwood.DEBUG)

import torrent
import peer
import piece


class TorrentClient:
    def __init__(self, torrent: torrent.Torrent):
        self._peer_id = struct.pack('!20s', b'-DV0001-')
        self._torrent = torrent
        self._piece_manager = piece.PieceManager(torrent=torrent)
        self._peer_manager = peer.PeerManager(
            self._torrent, self._peer_id, self._piece_manager
        )

    async def _start(self):
        await self._peer_manager.start()

    async def run(self):
        await self._start()
        await self._peer_manager.run()

    async def stop(self):
        pass


if __name__ == '__main__':
    logger = logwood.get_logger(__name__)
    t = torrent.Torrent.load_from_file('judas.torrent')
    client = TorrentClient(torrent=t)
    asyncio.run(client.run())
