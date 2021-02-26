from typing import List


class FileWriter:
    """
    Simple class that accepts bytes and writes them into a `path` file

    This had at first more complex logic so I moved it to class, but changed it later
    and left it here
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._file = open(path, 'wb')

    def add_piece(self, pieces_data: List[bytes]) -> None:
        for data in pieces_data:
            self._file.write(data)

    def close(self):
        self._file.close()
