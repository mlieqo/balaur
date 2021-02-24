from typing import Optional, List
import logwood
import struct

import bittorrent_message


class Parser:
    """
    Class for parsing raw data received from peers and creating `bittorrent_message`
    dataclasses from them.
    """

    MIN_MESSAGE_LENGTH = 5

    MESSAGE_TYPES = {
        0: bittorrent_message.Choke,
        1: bittorrent_message.Unchoke,
        2: bittorrent_message.Interested,
        5: bittorrent_message.BitField,
        6: bittorrent_message.Request,
        7: bittorrent_message.Piece,
    }

    def __init__(self):
        self._logger = logwood.get_logger(self.__class__.__name__)

    @staticmethod
    def get_length(data: bytes):
        """
        Get message length
        """
        (length,) = struct.unpack('!I', data[:4])
        return length

    def process_message(
        self, message: Optional[bytes]
    ) -> Optional[bittorrent_message.BaseMessage]:
        """
        Process messages based on their ID
        """
        if message is None or len(message) < self.MIN_MESSAGE_LENGTH:
            return

        length, message_id = struct.unpack('!IB', message[0:5])
        if length > len(message):
            self._logger.debug(
                'Received message with length - %d, but it should be - %d',
                len(message),
                length,
            )
            return

        try:
            return self.MESSAGE_TYPES[message_id].from_bytes(length, message)
        except KeyError:
            self._logger.error('Unknown message id = %s', message_id)
            return

    def process_handshake(self, response: bytes) -> Optional[List]:
        """
        Process handshake message. It's possible that peers will send bitfield message
        right after the handshake, so we return here List of messages.
        """
        if len(response) < bittorrent_message.Handshake.LENGTH:
            return

        messages = [bittorrent_message.Handshake.from_bytes(response)]

        # peers sometimes send also bitfield together with handshake
        if len(response) > bittorrent_message.Handshake.LENGTH:
            if isinstance(
                (
                    bitfield := self.process_message(
                        response[bittorrent_message.Handshake.LENGTH :]
                    )
                ),
                bittorrent_message.BitField,
            ):
                messages.append(bitfield)
        return messages
