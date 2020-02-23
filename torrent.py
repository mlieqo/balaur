import bencode
import hashlib
import struct
from urllib.parse import urlparse


class Torrent:
    def __init__(self, info, content, announce):
        self.info = info
        self.content = content
        self.announce = announce

    @classmethod
    def load_from_file(cls, filepath: str):
        content = cls.open_torrent_file(filepath)
        info = content['info']
        announce = urlparse(content['announce'])
        return cls(info, content, announce)

    def load_from_magnet(self):
        pass

    @property
    def info_hash(self):
        info = bencode.encode(self.info)
        info = hashlib.sha1(b'%s' % info).digest()
        return info

    @property
    def peer_id(self):
        # Maybe move somewhere else?
        return struct.pack('!20s', b'-DV0001-')

    @property
    def torrent_size(self):
        return sum([x['length'] for x in self.info['files']])

    @staticmethod
    def open_torrent_file(filepath):
        try:
            with open(filepath, 'rb') as f:
                torrent_file = bencode.decode(f.read())
                return torrent_file
        except Exception as e:
            raise TorrentFileError('Unable to read torrent file') from e


class TorrentFileError(Exception):
    pass
