import hashlib
import socket
import logging
from utils.consts import *
from typing import Dict, List
OK = 1

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

logger = logging.getLogger(__name__)

def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(),16)

def send_by_broadcast(message: str,closed=True, broadcast_port = BROADCAST_PORT):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.sendto(message.encode(), ('<broadcast>', broadcast_port))
    if closed:
        s.close()
    

def send_w_ack(first_msg: str, second_msg: str, target_ip: str, target_port: int):
        """Sends two messages to target ip, always waiting for OK ack"""
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_ip, target_port))
            s.sendall(first_msg.encode('utf-8'))
        
            ack = s.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK NEGATIVO")
            else:
                print("ACK POSITIVO PARA EL PRIMER MENSAJE")
            s.sendall(second_msg.encode('utf-8'))
            
            ack = s.recv(1024).decode('utf-8')
            # print(f'ACK: {ack}')
            if ack != f"{OK}":
                raise Exception(f"ACK NEGATIVO: {ack}")
            else:
                print("ACK POSITIVO PARA EL SEGUNDO MENSAJE")

def bully(ip1, ip2):
    return int(ip1.split('.')[-1]) > int(ip2.split('.')[-1])


def inbetween(k: int, start: int, end: int) -> bool:
        """Checks if k is in the interval (start, end].

        Args:
            k (int): Value to know if is between start and end
            start (int): Beginning of interval
            end (int): Ending of interval

        Returns:
            bool: True or false if is between or not
        """
        if start < end:
            return start < k <= end
        else:  # The interval wraps around 0
            return start < k or k <= end


