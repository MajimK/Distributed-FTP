from application.FTPNode import FTPNode
from communication.chord_node_reference import ChordNodeReference
from communication.self_discovery import SelfDiscovery
import socket

if __name__ == "__main__":

    ip = socket.gethostbyname(socket.gethostname())
    print(f"[IP]: {ip}")
    
    target_ip = SelfDiscovery(ip).find()
    node = FTPNode(ip)
    node.join(ChordNodeReference(target_ip))