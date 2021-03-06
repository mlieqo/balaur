from typing import ClassVar, Optional

import abc
import attr
import bitstring
import struct


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
    def to_bytes(piece_index: int, block_offset: int, block_length: int) -> bytes:
        length = struct.pack('!I', 13)
        message_id = struct.pack('!B', 6)
        index = struct.pack('!I', piece_index)
        begin = struct.pack('!I', block_offset)
        block_length = struct.pack('!I', block_length)
        return length + message_id + index + begin + block_length

    @classmethod
    def from_bytes(cls, length: int, buffer: bytes) -> 'Request':
        pass


@attr.s(slots=True, auto_attribs=True)
class Piece(BaseMessage):

    ID: ClassVar[int] = 7

    payload_length: int = attr.ib()
    message_id: int = attr.ib()
    index: int = attr.ib()
    block_offset: int = attr.ib()
    block_data: bytes = attr.ib()

    def to_bytes(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def from_bytes(cls, _: int, buffer: bytes) -> 'Piece':
        block_length = len(buffer) - 13
        (
            payload_length,
            message_id,
            piece_index,
            block_offset,
            block_data,
        ) = struct.unpack(f'!IBII{block_length}s', buffer[: 13 + block_length])

        return cls(payload_length, message_id, piece_index, block_offset, block_data)
