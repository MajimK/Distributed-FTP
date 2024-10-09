import socket
from application.FTPNode import FTPNode

ip = socket.gethostbyname(socket.gethostname())
FTPNode(ip)