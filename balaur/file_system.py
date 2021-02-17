import asyncio


class FileWriter:

    # number of seconds writer checks queue
    QUEUE_CHECK_SLEEP = 100

    def __init__(self, queue: asyncio.Queue, path: str) -> None:
        self._path = path
        self._queue = queue

    async def start(self):
        pass

    async def run(self):
        while True:
            file_part = self._queue.get()
