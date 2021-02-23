from typing import List, Optional
import dataclasses
import hashlib
import math

import bitstring
import cached_property
import logwood

import torrent


logger = logwood.get_logger(__name__)


class PieceManager:
    def __init__(self, torrent: torrent.Torrent):
        self._torrent = torrent
        self._piece_length = torrent.piece_length
        # initialize empty bitfield
        self._bitfield: bitstring.BitArray = bitstring.BitArray(
            length=self._torrent.number_of_pieces
        )
        self._pieces: List[Piece] = self._init_pieces()

    def _init_pieces(self):
        """
        Initialize dictionary with keys as piece numbers and values as dictionary
        with information if piece is currently being downloaded, is downloaded and
        data for specific blocks.
        """
        pieces = []

        for piece_number in range(self._torrent.number_of_pieces):
            hash_start_index = piece_number * Piece.HASH_LENGTH
            pieces.append(
                Piece(
                    index=piece_number,
                    piece_hash=self._torrent.info['pieces'][
                        hash_start_index : hash_start_index + Piece.HASH_LENGTH
                    ],
                    length=self._torrent.piece_length,
                )
            )
        return pieces

    def get_available_piece(
        self, peer_bitfield: bitstring.BitArray
    ) -> Optional['Piece']:
        for index, bit in enumerate(self._bitfield):
            if not bit and peer_bitfield[index]:
                # we invert bitfield, signaling that the piece index is either downloaded
                # or being downloaded
                self._bitfield.invert(index)
                return self._pieces[index]

    def return_piece_by_index(self, index: int) -> None:
        self._bitfield.invert(index)

    @property
    def number_of_pieces(self):
        return self._torrent.number_of_pieces


class Piece:

    HASH_LENGTH = 20

    def __init__(self, index: int, piece_hash, length: int):
        self.index = index
        self.block_index = 0
        self._hash = piece_hash
        self._length = length
        self._blocks = [
            Block(index)
            for index in range(math.ceil(float(self._length / Block.LENGTH)))
        ]

    def add_block_data(self, data: bytes) -> bool:
        """
        Adds data to piece block. If all blocks were downladed, checks validity/hash of itself.
        In case piece is valid return True, otherwise reset all block data and return False.
        """

        self._blocks[self.block_index].data = data

        if self.block_index == self.block_count - 1:
            # we downloaded all blocks for this piece
            if self._is_hash_correct():
                logger.info('Downloaded piece n. %d', self.index)
                return True
            else:
                # we clear all blocks if piece is not valid
                logger.debug('Piece n.%d not valid', self.index)
                for block in self._blocks:
                    block.data = None
                self.block_index = 0
        else:
            self.block_index += 1

        return False

    @property
    def current_block_offset(self):
        return self.block_index * Block.LENGTH

    @property
    def current_block_length(self):
        # if last block
        if self.block_index == self.block_count - 1:
            length = self._length - (self.block_count - 1) * Block.LENGTH
            return length
        return Block.LENGTH

    def _is_hash_correct(self):
        piece_data = b''
        for block in self._blocks:
            piece_data += block.data
        data_hash = hashlib.sha1(piece_data).digest()
        return self._hash == data_hash

    @cached_property.cached_property
    def block_count(self):
        return len(self._blocks)


@dataclasses.dataclass
class Block:

    LENGTH = 2 ** 14

    def __init__(self, index: int):
        self.index = index
        self.offset = index * self.LENGTH
        self.data = None
