import bencode
import socket
import urllib.parse
import struct
import random
import hashlib


def main():
    req_MESSAGE, transaction_id = create_conn_req()

    with open('ford.torrent', 'rb') as f:
        torrent = bencode.decode(f.read())

    import pprint
    p = pprint.PrettyPrinter(indent=6)

    torrent_announce = urllib.parse.urlparse(torrent['announce'])

    CONNECTION = (torrent_announce.hostname,
                  torrent_announce.port)
    connection_id = None

    while not connection_id:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
        sock.settimeout(1.0)
        sock.sendto(req_MESSAGE, CONNECTION)
        try:
            buf = sock.recvfrom(2048)[0]
            connection_id = parse_conn_response(buf, transaction_id)
        except socket.timeout:
            print('REQUEST TIMED OUT')

    announce_res = create_announce_req(connection_id, port=777, torrent['info'])
    print(connection_id)


def create_conn_req():
    connection_id = 0x41727101980  # default connection id
    action = 0x0  # action (0 = give me a new connection id)
    transaction_id = random.randrange(0, 255)  # randomized by client
    buf = struct.pack("!q", connection_id)  # first 8 bytes is connection id
    buf += struct.pack("!i", action)  # next 4 bytes is action
    buf += struct.pack("!i", transaction_id)  # next 4 bytes is transaction id
    return buf, transaction_id


def parse_conn_response(buffer, transaction_id):
    action = struct.unpack_from("!i", buffer)[0]  # first 4 bytes is action

    res_transaction_id = struct.unpack_from("!i", buffer, 4)[0]  # next 4 bytes is transaction id
    if res_transaction_id != transaction_id:
        raise RuntimeError(
            "Transaction ID doesnt match in connection response!"
            " Expected %s, got %s" % (transaction_id, res_transaction_id)
        )
    print(action)
    print(res_transaction_id)
    if action == 0:
        connection_id = struct.unpack_from("!q", buffer, 8)[0]  # unpack 8 bytes from byte 8, should be the connection_id
        return connection_id
    elif action == 3:
        error = struct.unpack_from("!s", buffer, 8)
        raise RuntimeError("Error while trying to get a connection response: %s" % error)


def create_announce_req(connection_id, port, torrent_info):
    action = 0x1  # action (1 = announce)
    transaction_id = random.randrange(0, 255)
    buf = struct.pack("!q", connection_id)  # first 8 bytes is connection id
    buf += struct.pack("!i", action)  # next 4 bytes is action
    buf += struct.pack("!i", transaction_id)  # followed by 4 byte transaction id
    buf += struct.pack("!20s", info_hash(torrent_info))  # the info hash of the torrent we announce ourselves in
    buf += struct.pack("!20s", peer_id())  # the peer_id we announce
    buf += struct.pack("!q", int(urllib.unquote(payload['downloaded'])))  # number of bytes downloaded
    buf += struct.pack("!q", int(urllib.unquote(payload['left'])))  # number of bytes left
    buf += struct.pack("!q", int(urllib.unquote(payload['uploaded'])))  # number of bytes uploaded
    buf += struct.pack("!i", 0x2)  # event 2 denotes start of downloading
    buf += struct.pack("!i", 0x0)  # IP address set to 0. Response received to the sender of this packet
    key = random.randrange(0, 255)  # Unique key randomized by client
    buf += struct.pack("!i", key)
    buf += struct.pack("!i", -1)  # Number of peers required. Set to -1 for default
    buf += struct.pack("!i", s_port)  # port on which response will be sent
    return (buf, transaction_id)


def peer_id():
    return '-DV0001-'


def info_hash(torrent_info):
    info = bencode.encode(torrent_info)
    return hashlib.sha1(b'%s' % info).hexdigest()


if __name__ == '__main__':
    main()