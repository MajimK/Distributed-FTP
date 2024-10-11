import socket
from dht.chord import ChordNode

import hashlib

def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(),16)

if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())
    
    print(f"[IP]: {ip} --- [HASH]: {getShaRepr(ip)}")
    
    node = ChordNode(ip)
    print(f"[IP]: {ip}")

    node.join()


