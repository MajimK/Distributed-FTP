from utils.operations import *
import socket
COORDINATOR_PORT = 5050
DEFAULT_PORT = 8001
ELECTOR_PORT = 8002
BROADCAST_PORT = 8005
DATABASE_PORT = 8006
FTP_PORT = 21
DATA_TRANSFER_PORT = 20
PROXY_IP = socket.gethostbyname(socket.gethostname())
PROXY_PORT = 2121
ROOT = '/app/database'
END = 'XXXXXXXXXXENDXXXXXXXXXX'

commands = [CWD,
            DELE,
            LIST,
            MKD,
            PASV,
            PORT,
            PWD,
            RETR,
            RMD,
            STOR,
            TYPE_A,
            TYPE_I,
            SYST,
            QUIT]