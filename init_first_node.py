import socket
from application.FTPNode import FTPNode

if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())

    node = FTPNode(ip)
    print(f"[IP]: {ip}")

    node.join()


