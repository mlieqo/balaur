import asyncio
import struct

import torrent
import peer
import piece


t = torrent.Torrent.load_from_file('condor.torrent')
CONNECTION = (t.announce_list[2].hostname, t.announce_list[2].port)

print(CONNECTION)
import pprint
p = pprint.PrettyPrinter(indent=6)
blocks = []


class TorrentClient:
    def __init__(self, torrent_: torrent.Torrent):
        self._peer_id = struct.pack('!20s', b'-DV0001-')
        self._torrent = torrent_
        self._piece_manager = piece.PieceManager(piece_length=torrent_.piece_length, torrent=t)
        self._peer_manager = peer.PeerManager(self._torrent, self._peer_id, self._piece_manager)

    async def _start(self):
        await self._peer_manager.start()

    async def run(self):
        await self._start()
        await self._peer_manager.run()

    async def stop(self):
        pass


if __name__ == '__main__':
    client = TorrentClient(torrent_=t)
    asyncio.run(client.run())
