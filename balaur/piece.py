from typing import List, Optional, Set
import dataclasses
import hashlib
import math
import os

import bitstring
import cached_property
import logwood

import balaur.writer
import balaur.torrent


class PieceManager:
    """
    Manager class that holds information about the individual pieces.

    Writing to the actual file is also managed on every successful piece download.
    """

    def __init__(self, torrent: balaur.torrent.Torrent, destination_directory: str):
        self._torrent = torrent
        self._file_writer = balaur.writer.FileWriter(
            os.path.join(destination_directory, self._torrent.info['name'])
        )
        self._piece_length = torrent.piece_length
        # initialize empty bitfield
        self._bitfield: bitstring.BitArray = bitstring.BitArray(
            length=self._torrent.number_of_pieces
        )
        self._pieces: List[Piece] = self._init_pieces()
        # set of piece indexes that are currently being downloaded
        self._pieces_downloading: Set[int] = set()
        self._last_piece_index_written = -1
        self._logger = logwood.get_logger(self.__class__.__name__)

    def _init_pieces(self) -> List['Piece']:
        """
        Initialize dictionary with keys as piece numbers and values as dictionary
        with information if piece is currently being downloaded, is downloaded and
        data for specific blocks.
        """
        pieces = []
        for piece_index in range(self._torrent.number_of_pieces):
            hash_start_index = piece_index * Piece.HASH_LENGTH
            if piece_index + 1 == self._torrent.number_of_pieces:
                piece_length = (
                    self._torrent.torrent_size
                    - (self._torrent.number_of_pieces - 1) * self._torrent.piece_length
                )
            else:
                piece_length = self._torrent.piece_length
            pieces.append(
                Piece(
                    index=piece_index,
                    piece_hash=self._torrent.info['pieces'][
                        hash_start_index : hash_start_index + Piece.HASH_LENGTH
                    ],
                    length=piece_length,
                )
            )
        return pieces

    def validate_piece(self, piece: 'Piece') -> bool:
        """
        Checks if hash of downloaded piece is correct. If yes, it finds the longest
        piece chain possible for writing and send is to `self._file_writer`. In case
        it's incorrect, the piece block data gets flushed.

        If all of the pieces have been downloaded, `NoAvailablePieces` is raised.
        """
        data_hash = hashlib.sha1(piece.get_block_data()).digest()
        if is_valid := piece.hash == data_hash:
            self._logger.debug('Downloaded piece n. %d', piece.index)
            self._pieces_downloading.remove(piece.index)

            pieces_to_be_written = []
            for bit in self._bitfield[self._last_piece_index_written + 1 :]:
                piece_index = self._last_piece_index_written + 1
                if bit and piece_index not in self._pieces_downloading:
                    pieces_to_be_written.append(
                        self._pieces[piece_index].get_block_data()
                    )
                    # drop ref count
                    self._pieces[piece_index] = None
                    self._last_piece_index_written = piece_index
                else:
                    if pieces_to_be_written:
                        self._file_writer.add_piece(pieces_to_be_written)
                    break
            else:
                if pieces_to_be_written:
                    self._file_writer.add_piece(pieces_to_be_written)
                self._file_writer.close()
                raise NoAvailablePieces
        else:
            piece.clear_block_data()

        return is_valid

    def get_available_piece(
        self, peer_bitfield: bitstring.BitArray
    ) -> Optional['Piece']:
        """
        Return the piece that the peer is holding, and is not downloaded or being downloaded.

        Raises `NoPiecesAvailable` if all the pieces are already downloaded.
        """
        if self._last_piece_index_written == self.number_of_pieces - 1:
            raise NoAvailablePieces

        for index, bit in enumerate(self._bitfield):
            if not bit and peer_bitfield[index]:
                # we invert bitfield, signaling that the piece index is either downloaded
                # or being downloaded
                self._bitfield.invert(index)
                self._pieces_downloading.add(index)
                return self._pieces[index]

    def return_piece_by_index(self, index: int) -> None:
        """
        Remove the piece from pieces downloading set.
        """
        self._pieces_downloading.remove(index)
        self._bitfield.invert(index)

    @property
    def number_of_pieces(self) -> int:
        return self._torrent.number_of_pieces


class NoAvailablePieces(Exception):
    """ When all pieces have been downloaded """


class Piece:
    """
    Torrent file data piece, composed from individual block -> `self._blocks`.
    """

    HASH_LENGTH = 20

    def __init__(self, index: int, piece_hash, length: int):
        self.index = index
        self.block_index = 0
        self.hash = piece_hash
        self._length = length
        self._blocks = [
            Block(index)
            for index in range(math.ceil(float(self._length / Block.LENGTH)))
        ]

    def add_block_data(self, data: bytes) -> bool:
        """
        Adds data to piece block. Returns True, if all blocks were downloaded.
        """

        self._blocks[self.block_index].data = data

        if self.block_index == self.block_count - 1:
            # we downloaded all blocks for this piece
            return True
        self.block_index += 1
        return False

    def clear_block_data(self) -> None:
        for block in self._blocks:
            block.data = None
        self.block_index = 0

    def get_block_data(self) -> bytes:
        piece_data = b''
        for block in self._blocks:
            piece_data += block.data
        return piece_data

    @property
    def current_block_offset(self):
        return self._blocks[self.block_index].offset

    @property
    def current_block_length(self):
        # if last block
        if self.block_index == self.block_count - 1:
            length = self._length - (self.block_count - 1) * Block.LENGTH
            return length
        return Block.LENGTH

    @cached_property.cached_property
    def block_count(self):
        return len(self._blocks)


@dataclasses.dataclass
class Block:

    __slots__ = ['index', 'offset', 'data']

    LENGTH = 2 ** 14

    def __init__(self, index: int):
        self.index = index
        self.offset = index * self.LENGTH
        self.data = None
