import bencode
import hashlib
import math
from cached_property import cached_property
from urllib.parse import urlparse


class Torrent:
    def __init__(self, content, announce, announce_list):
        self.content = content
        self.info = content['info']
        self.announce = announce
        self.announce_list = announce_list
        self.piece_length = self.info['piece length']

    @cached_property
    def torrent_size(self):
        try:
            return self.info['length']
        except KeyError:
            return sum([x['length'] for x in self.info['files']])

    @cached_property
    def number_of_pieces(self) -> int:
        return math.ceil(self.torrent_size / self.piece_length)

    @cached_property
    def info_hash(self):
        info_hash = bencode.encode(self.info)
        info_hash = hashlib.sha1(b'%s' % info_hash).digest()
        return info_hash

    @classmethod
    def load_from_file(cls, filepath: str):
        content = cls.open_torrent_file(filepath)
        announce = urlparse(content['announce'])
        announce_list = [urlparse(x[0]) for x in content['announce-list']]
        return cls(
            content,
            announce,
            announce_list
        )

    def load_from_magnet(self):
        pass

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
