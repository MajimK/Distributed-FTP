import hashlib
import socket
import logging
from consts import *

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

logger = logging.getLogger(__name__)

def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(),16)

def send_by_broadcast(message: str,closed=True):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.sendto(message.encode(), ('<broadcast>', BROADCAST_PORT))
    if closed:
        s.close()
    

def bully(ip1, ip2):
    return int(ip1.split('.')[-1]) > int(ip2.split('.')[-1])
