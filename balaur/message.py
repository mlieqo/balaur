from typing import ClassVar, Optional, List

import abc
import attr
import bitstring
import logging
import random
import socket
import struct

import piece


class BaseMessage(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def from_bytes(cls, **kwargs):
        """ Loads message from response """

    @abc.abstractmethod
    def to_bytes(self, **kwargs):
        """ Create new message for sending"""


@attr.s(slots=True, auto_attribs=True)
class Handshake:

    PSTRLEN = 19
    PSTR = b'BitTorrent protocol'

    LENGTH: ClassVar[int] = 49 + PSTRLEN

    peer_id: bytes = attr.ib()
    info_hash: bytes = attr.ib()

    @classmethod
    def to_bytes(cls, peer_id, info_hash) -> bytes:
        pstrlen = struct.pack('!B', cls.PSTRLEN)
        pstr = struct.pack(f'!{cls.PSTRLEN}s', cls.PSTR)
        info_hash = struct.pack('!20s', info_hash)
        peer_id = struct.pack('!20s', peer_id)
        reserved = struct.pack('!Q', 0)
        return pstrlen + pstr + reserved + info_hash + peer_id

    @classmethod
    def from_bytes(cls, buffer: bytes) -> Optional['Handshake']:
        info_hash, peer_id = struct.unpack('!20s20s', buffer[28 : cls.LENGTH])
        return cls(peer_id=peer_id, info_hash=info_hash)


@attr.s(slots=True, auto_attribs=True)
class Choke(BaseMessage):
    ID: ClassVar[int] = 0

    def to_bytes(self) -> bytes:
        pass

    @classmethod
    def from_bytes(cls, *_) -> 'Choke':
        return cls()


@attr.s(slots=True, auto_attribs=True)
class Unchoke(BaseMessage):
    ID: ClassVar[int] = 1

    @staticmethod
    def to_bytes() -> bytes:
        length = struct.pack('!I', 1)
        message_id = struct.pack('!B', 2)
        return length + message_id

    @classmethod
    def from_bytes(cls, *_) -> 'Unchoke':
        return cls()


@attr.s(slots=True, auto_attribs=True)
class Interested(BaseMessage):
    ID: ClassVar[int] = 2

    @staticmethod
    def to_bytes() -> bytes:
        length = struct.pack('!I', 1)
        message_id = struct.pack('!B', 2)
        return length + message_id

    @classmethod
    def from_bytes(cls, *_) -> 'Interested':
        return cls()


@attr.s(slots=True, auto_attribs=True)
class BitField(BaseMessage):

    ID: ClassVar[int] = 5

    length: int = attr.ib()
    bitfield: bitstring.BitArray = attr.ib()

    def to_bytes(self) -> bytes:
        pass

    @classmethod
    def from_bytes(cls, length: int, buffer: bytes) -> 'BitField':
        bitfield_length = length - 1
        (raw_bitfield,) = struct.unpack(
            f'!{bitfield_length}s', buffer[5 : 5 + bitfield_length]
        )
        bitfield = bitstring.BitArray(bytes=bytes(raw_bitfield))
        return cls(length, bitfield)


@attr.s(slots=True, auto_attribs=True)
class Request(BaseMessage):

    ID: ClassVar[int] = 6

    @staticmethod
    def to_bytes(piece_index, block_offset) -> bytes:
        length = struct.pack('!I', 13)
        message_id = struct.pack('!B', 6)
        index = struct.pack('!I', piece_index)
        begin = struct.pack('!I', block_offset)
        block_length = struct.pack('!I', piece.Block.LENGTH)
        return length + message_id + index + begin + block_length

    @classmethod
    def from_bytes(cls, length: int, buffer: bytes) -> 'Request':
        pass


@attr.s(slots=True, auto_attribs=True)
class Piece(BaseMessage):

    ID: ClassVar[int] = 7

    index: int = attr.ib()
    block_offset: int = attr.ib()
    block_data: bytes = attr.ib()

    def to_bytes(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def from_bytes(cls, _: int, buffer: bytes) -> 'Piece':
        block_length = len(buffer) - 13
        piece_index, block_offset, block_data = struct.unpack(
            f'II{block_length}s', buffer[5 : 13 + block_length]
        )
        return cls(piece_index, block_offset, block_data)


class MessageHandler:

    MIN_MESSAGE_LENGTH = 5

    MESSAGE_TYPES = {
        0: Choke,
        1: Unchoke,
        2: Interested,
        5: BitField,
        6: Request,
        7: Piece,
    }

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def process_message(self, message: bytes) -> Optional[BaseMessage]:
        if len(message) < self.MIN_MESSAGE_LENGTH:
            return

        length, message_id = struct.unpack('!IB', message[0:5])
        try:
            return self.MESSAGE_TYPES[message_id].from_bytes(length, message)
        except KeyError:
            self._logger.error('Unknown message id = %s', message_id)
            return

    def process_handshake(self, response: Optional[bytes]) -> Optional[List]:
        """ Individual handle for handshake as it's different from other messages """
        if response is None or len(response) < Handshake.LENGTH:
            return

        messages = [Handshake.from_bytes(response)]

        # peers sometimes send also bitfield together with handshake
        if len(response) > Handshake.LENGTH:
            if isinstance(
                (bitfield := self.process_message(response[Handshake.LENGTH :])),
                BitField,
            ):
                messages.append(bitfield)
        return messages


class UDPTrackerConnection:
    def __init__(self):
        self._transaction_id = random.randrange(0, 10000)
        self.message = self._build_message()

    def _build_message(self):
        connection_id = struct.pack('!Q', 0x41727101980)
        action = struct.pack("!I", 0)
        transaction_id = struct.pack("!I", self._transaction_id)
        buffer = connection_id + action + transaction_id
        return buffer

    def read(self, buffer):
        (action,) = struct.unpack_from("!I", buffer)
        (res_transaction_id,) = struct.unpack_from("!I", buffer, 4)
        if res_transaction_id != self._transaction_id:
            raise RuntimeError('tr id')
        if action == 0:
            (connection_id,) = struct.unpack_from("!q", buffer, 8)
            return connection_id
        else:
            raise RuntimeError('wrong action')


class UDPTrackerAnnounce:
    def __init__(self, connection_id, info_hash, peer_id, torrent_size):
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.torrent_size = torrent_size
        self.connection_id = connection_id
        self.transaction_id = random.randrange(0, 10000)

    def build_msg(self):
        # self.connection_id = connection_id
        connection_id = struct.pack('!Q', self.connection_id)
        action = struct.pack('!I', 1)
        transaction_id = struct.pack('!I', self.transaction_id)
        downloaded = struct.pack('!Q', 0)
        t_size = struct.pack('!Q', self.torrent_size)
        uploaded = struct.pack('!Q', 0)
        event = struct.pack('!I', 2)
        ip = struct.pack('!I', 0)
        key = struct.pack('!I', random.randrange(0, 255))
        num_peers = struct.pack('!i', -1)
        port = struct.pack('!H', 6889)
        return (
            connection_id
            + action
            + transaction_id
            + self.info_hash
            + self.peer_id
            + downloaded
            + t_size
            + uploaded
            + event
            + ip
            + key
            + num_peers
            + port
        )

    @staticmethod
    def read(buffer):
        (action,) = struct.unpack(">I", buffer[0:4])  # first 4 bytes is action

        if action == 1:
            ret = dict()
            ret['action'] = action
            (res_transaction_id,) = struct.unpack(
                "!I", buffer[4:8]
            )  # next 4 bytes is transaction id
            ret['transaction_id'] = res_transaction_id
            (ret['interval'],) = struct.unpack("!I", buffer[8:12])
            (ret['leeches'],) = struct.unpack("!I", buffer[12:16])
            (ret['seeds'],) = struct.unpack("!I", buffer[16:20])
            peers = set()
            x = 0
            offset = 20
            while offset != len(buffer):
                ip = struct.unpack_from("!I", buffer, offset)[0]
                ip = socket.inet_ntoa(struct.pack('!I', ip))
                offset += 4
                port = struct.unpack_from("!H", buffer, offset)[0]
                peers.add((ip, port))
                offset += 2
                x += 1
            return ret, peers