from chord.chord import ChordNode
from chord.chord_node_reference import ChordNodeReference
from ipaddress import ip_address
import socket
import sys
#172.17.0.2
#172.17.0.6
if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())

    # Create node
    node = ChordNode(ip)
    print(f"[IP]: {ip}")

    # Single node case
    if len(sys.argv) == 1:
        node.join()

    # Join node case
    elif len(sys.argv) == 2:
        try:
            target_ip = ip_address(sys.argv[1])
        except:
            raise Exception(f"Parameter {sys.argv[1]} is not a valid IP address")
        
        node.join(ChordNodeReference(sys.argv[1]))
    else:
        raise Exception("Incorrect params")

    while True:
        pass