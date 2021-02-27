import struct

import logwood

logwood.basic_config(level=logwood.DEBUG)

import balaur.torrent  # noqa
import balaur.peer  # noqa
import balaur.piece  # noqa


class TorrentClient:
    def __init__(self, torrent_file: str, destination: str):
        self._peer_id = struct.pack('!20s', b'-DV0001-')
        self._torrent = balaur.torrent.Torrent.load_from_file(torrent_file)
        self._piece_manager = balaur.piece.PieceManager(
            torrent=self._torrent, destination_directory=destination
        )
        self._peer_manager = balaur.peer.PeerManager(
            self._torrent, self._peer_id, self._piece_manager
        )

    async def _start(self):
        await self._peer_manager.start()

    async def run(self):
        await self._start()
        await self._peer_manager.run()

    async def stop(self):
        pass
