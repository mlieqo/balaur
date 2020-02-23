import struct
import random


PSTRLEN = 19
PSTR = b'BitTorrent protocol'


class Handshake:

    def __init__(self, peer_id, info_hash):
        self.peer_id = peer_id
        self.info_hash = info_hash

    def build_msg(self):
        pstrlen = struct.pack('!B', PSTRLEN)
        pstr = struct.pack(f'!{PSTRLEN}s', PSTR)
        info_hash = struct.pack('!20s', self.info_hash)
        peer_id = struct.pack('!20s', self.peer_id)
        reserved = struct.pack('!Q', 0)
        return pstrlen + pstr + reserved + info_hash + peer_id

    @classmethod
    def read(cls, buffer):
        if len(buffer) < 68:
            raise RuntimeError('Wrong handshake length')
        info_hash, = struct.unpack('!20s', buffer[28:48])
        peer_id, = struct.unpack('!20s', buffer[48:68])
        return cls(
            peer_id=peer_id,
            info_hash=info_hash
        )


class BitField:
    def __init__(self, length, bitfield):
        self.id = 5
        self.length = length
        self.bitfield = bitfield

    @classmethod
    def read(cls, buffer):
        length, = struct.unpack('!I', buffer[0:4])
        bitfield, = struct.unpack(f'!{int(length)}s', buffer[5:5+int(length)-1])
        return cls(length, bitfield)


class UDPTrackerConnection:
    def __init__(self):
        self.transaction_id = random.randrange(0, 10000)

    def build_msg(self):
        connection_id = struct.pack('!Q', 0x41727101980)
        action = struct.pack("!I", 0)
        transaction_id = struct.pack("!I", self.transaction_id)
        buffer = connection_id + action + transaction_id
        return buffer

    def read(self, buffer):
        action, = struct.unpack_from("!I", buffer)
        res_transaction_id, = struct.unpack_from("!I", buffer, 4)
        if res_transaction_id != self.transaction_id:
            raise RuntimeError('tr id')
        if action == 0:
            connection_id, = struct.unpack_from("!q", buffer, 8)
            return connection_id
        else:
            raise RuntimeError('wrong action')


class UDPTrackerAnnounce:
    def __init__(self, connection_id, info_hash,
                 peer_id):
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.connection_id = connection_id
        self.transaction_id = random.randrange(0, 10000)

    def build_msg(self, torrent_size):
        connection_id = struct.pack('!Q', self.connection_id)
        action = struct.pack('!I', 1)
        transaction_id = struct.pack('!I', self.transaction_id)
        downloaded = struct.pack('!Q', 0)
        t_size = struct.pack('!Q', torrent_size)
        uploaded = struct.pack('!Q', 0)
        event = struct.pack('!I', 2)
        ip = struct.pack('!I', 0)
        key = struct.pack('!I', random.randrange(0, 255))
        num_peers = struct.pack('!i', -1)
        port = struct.pack('!H', 6889)
        return (connection_id + action + transaction_id + self.info_hash +
                self.peer_id + downloaded + t_size + uploaded + event + ip +
                key + num_peers + port)

    def read(self):
        pass