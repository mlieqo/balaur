from typing import List

import hashlib
import logging
import cached_property


logger = logging.getLogger(__name__)


class PieceManager:

    def __init__(self, piece_length, torrent):
        self.piece_length = piece_length
        self.torrent = torrent
        self.pieces: List[Piece] = self._init_pieces()

    def _init_pieces(self):
        """
        Initialize dictionary with keys as piece numbers and values as dictionary
        with information if piece is currently being downloaded, is downloaded and
        data for specific blocks.
        """
        pieces = []

        for piece_number in range(self.torrent.number_of_pieces):
            hash_start_index = piece_number * Piece.HASH_LENGTH
            pieces.append(
                Piece(
                    index=piece_number,
                    hash_=self.torrent.info['pieces'][hash_start_index:hash_start_index + Piece.HASH_LENGTH],
                    length=self.torrent.piece_length,
                )
            )
        return pieces

    def get_available_piece(self, bitfield=None):
        # FIXME: slow
        for piece_ in self.pieces:
            if not piece_.downloaded and not piece_.queued:
                if bitfield:
                    print('there is bifield')
                if bitfield is None or (bitfield is not None and bitfield[piece_.index]):
                    piece_.queued = True
                    return piece_


class Piece:

    HASH_LENGTH = 20

    def __init__(self, index, hash_, length):
        self.index = index
        self.hash = hash_
        self.length = length
        self.downloaded = False
        self.queued = False
        self.blocks = [Block(index) for index in range(int(self.length / Block.LENGTH))]

    def validate(self) -> None:
        if self._is_hash_correct():
            self.downloaded = True
            print(f'Downloaded piece n. {self.index}')
        else:
            # we clear all blocks if piece is not valid
            for block in self.blocks:
                block.data = None
        self.queued = False

    def _is_hash_correct(self):
        piece_data = b''
        for block in self.blocks:
            piece_data += block.data
        return self.hash == hashlib.sha1(piece_data).digest()

    @cached_property.cached_property
    def block_count(self):
        return len(self.blocks)


class Block:

    LENGTH = 2**14

    def __init__(self, index):
        self.index = index
        self.offset = index * self.LENGTH
        self.data = None
