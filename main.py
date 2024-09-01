from dht.chord import ChordNode
from communication.chord_node_reference import ChordNodeReference
import socket
import sys
from communication.self_discovery import SelfDiscovery
from application.FTPNode import FTPNode

if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())

    if len(sys.argv) == 1:
        # Create node
        node = FTPNode(ip)
        print(f"[IP]: {ip}")

        # Single node case
        node.join()

    # Join node case
    elif len(sys.argv) == 2 and sys.argv[1] == '-s':
        target_ip = SelfDiscovery(ip).find()
        print("SELF_D AQUI")
        node = FTPNode(ip)
        print("CN AQUI")
        node.join(ChordNodeReference(target_ip))
        print("JOIN AQUI")

    else:
        raise Exception("Incorrect params")

    while True:
        pass