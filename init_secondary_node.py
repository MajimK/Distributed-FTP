from dht.chord import ChordNode
from communication.chord_node_reference import ChordNodeReference
from communication.self_discovery import SelfDiscovery
import socket
import hashlib

def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(),16)

if __name__ == "__main__":

    ip = socket.gethostbyname(socket.gethostname())
    print(f"[IP]: {ip} --- [HASH]: {getShaRepr(ip)}")
    
    target_ip = SelfDiscovery(ip).find()
    node = ChordNode(ip)
    node.join(ChordNodeReference(target_ip))
