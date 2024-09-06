from operations import *

DEFAULT_PORT = 8001
BROADCAST_PORT = 8005
DATABASE_PORT = 8006
FTP_PORT = 21
DATA_TRANSFER_PORT = 20
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