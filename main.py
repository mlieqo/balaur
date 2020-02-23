import socket
import struct
import random
import asyncio
from torrent import Torrent
from message import Handshake, UDPTrackerConnection


t = Torrent.load_from_file('dark_waters.torrent')
CONNECTION = (t.announce.hostname, t.announce.port)


def main():
    udp_conn = UDPTrackerConnection()
    msg = udp_conn.build_msg()

    connection_id = None

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    while not connection_id:
        try:
            sock.settimeout(1.0)
            sock.sendto(msg, CONNECTION)
            buf = sock.recvfrom(2048)[0]
            connection_id = udp_conn.read(buf)
        except:
            pass

    #Annoucing
    req, transaction_id = create_announce_req(connection_id)
    yolo = None

    while not yolo:
        try:
            sock.sendto(req, CONNECTION)
            print("Announce Request Sent")
            buf = sock.recvfrom(2048)[0]
            print("Response received")
            yolo = parse_announce_res(buf)
        except:
            pass
    return yolo[1]
    # peer_handshake(yolo[1])


# def create_announce_req(connection_id):
    # action = 0x1  # action (1 = announce)
    # transaction_id = random.randrange(0, 255)
    # buf = struct.pack("!Q", connection_id)  # first 8 bytes is connection id
    # buf += struct.pack("!I", action)  # next 4 bytes is action
    # buf += struct.pack("!I", transaction_id)  # followed by 4 byte transaction id
    # buf += t.info_hash  # the info hash of the torrent we announce ourselves in
    # buf += t.peer_id  # the peer_id we announce
    # buf += struct.pack("!Q", 0)  # number of bytes downloaded
    # buf += struct.pack("!Q", t.torrent_size)  # number of bytes left
    # buf += struct.pack("!Q", 0)  # number of bytes uploaded
    # buf += struct.pack("!I", 2)  # event 2 denotes start of downloading
    # buf += struct.pack("!I", 0)  # IP address set to 0. Response received to the sender of this packet
    # key = random.randrange(0, 255)  # Unique key randomized by client
    # buf += struct.pack("!I", key)
    # buf += struct.pack("!i", -1)  # Number of peers required. Set to -1 for default
    # buf += struct.pack("!H", 6889)  # port on which response will be sent
    # return buf, transaction_id


def parse_announce_res(buf):
    action, = struct.unpack(">I", buf[0:4])  # first 4 bytes is action

    if action == 1:
        ret = dict()
        ret['action'] = action
        res_transaction_id, = struct.unpack("!I", buf[4:8])  # next 4 bytes is transaction id
        ret['transaction_id'] = res_transaction_id
        ret['interval'], = struct.unpack("!I", buf[8:12])
        ret['leeches'], = struct.unpack("!I", buf[12:16])
        ret['seeds'], = struct.unpack("!I", buf[16:20])
        peers = list()
        x = 0
        offset = 20
        while offset != len(buf):

            IP = struct.unpack_from("!I", buf, offset)[0]
            IP = socket.inet_ntoa(struct.pack('!I', IP))
            offset += 4
            port = struct.unpack_from("!H", buf, offset)[0]
            peers.append((IP, port))
            offset += 2
            x += 1
        return ret, peers


async def create_task_from_peer_list(peer_list):
    tasks = []
    for peer in peer_list:
        task = asyncio.create_task(async_peer_handshake(peer))
        tasks.append(task)
    result = await asyncio.gather(*tasks, return_exceptions=True)

    return result


async def async_peer_handshake(peer):
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(*peer), timeout=0.5
        )
        handshake = Handshake(t.peer_id, t.info_hash)
        msg = handshake.build_msg()
        writer.write(msg)
        print(f'handshake send {msg}')
        await writer.drain()

        data = await asyncio.wait_for(reader.read(4096), timeout=0.5)
        if data:
            print(f'received data {data}')

        writer.close()
        await writer.wait_closed()

    except Exception as e:
        raise e
    return True


if __name__ == '__main__':
    peer_list = main()
    result = asyncio.run(create_task_from_peer_list(peer_list))
    print(len([x for x in result if x is True]))
