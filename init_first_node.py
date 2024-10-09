import socket
from dht.chord import ChordNode
if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())

    node = ChordNode(ip)
    print(f"[IP]: {ip}")

    node.join()


