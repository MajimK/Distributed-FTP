from chord.chord import ChordNode
from chord.chord_node_reference import ChordNodeReference
import socket
import sys
from self_discovery import SelfDiscovery

if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())

    if len(sys.argv) == 1:
        # Create node
        node = ChordNode(ip)
        print(f"[IP]: {ip}")

        # Single node case
        node.join()

    # Join node case
    elif len(sys.argv) == 2 and sys.argv[1] == '-s':
        target_ip = SelfDiscovery(ip).find()
        print("SELF_D AQUI")
        node = ChordNode(ip)
        print("CN AQUI")
        node.join(ChordNodeReference(target_ip))
        print("JOIN AQUI")

    else:
        raise Exception("Incorrect params")

    while True:
        pass