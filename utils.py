import hashlib
import socket

def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(),16)

def send_by_broadcast(message: str, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.sendto(message.encode(), ('<broadcast>', port))
    s.close()