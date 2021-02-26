import asyncio
import click
import balaur.client


@click.command()
@click.option('-t', '--torrent', help='Path to a torrent file')
@click.option('-d', '--destination', help='Download destination directory')
def main(torrent: str, destination: str) -> None:
    torrent_client = balaur.client.TorrentClient(torrent, destination)
    asyncio.run(torrent_client.run())


if __name__ == '__main__':
    main()
