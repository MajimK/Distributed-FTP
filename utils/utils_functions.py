import hashlib
import socket
import logging
from utils.consts import *
from typing import Dict, List
from utils.operations import OK

logging.basicConfig(level=logging.DEBUG,
                    format='%(threadName)s - %(filename)s - %(funcName)s - %(message)s')

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


def send_replication_message(operation, args, port, successor_ip, predecessor_ip = None):
    send_w_ack(operation, args, successor_ip, port)
    
    if predecessor_ip and predecessor_ip != successor_ip:
        send_w_ack(operation, args, predecessor_ip, port)


def secure_send(msg:str, target_ip: str, target_port: str, tries):
     if tries == 0:
         return False
     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((target_ip, target_port))
        s.sendall(f'{msg}'.encode('utf-8'))

        ack = s.recv(1024).decode('utf-8')
        if ack != f'{OK}':
            print("NO ACK")
            return secure_send(msg, target_ip, target_port, tries-1)
        else: return True

        
# def find_coordinator() -> str:
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
#         s.sendto(FIND.encode(),('<broadcast>', BROADCAST_PORT))

#         coordinator_ip = s.recv(1024).decode().strip()

#         return coordinator_ip

def find(message) -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    s.sendto(message.encode(),('<broadcast>', BROADCAST_PORT))

    response = s.recv(1024).decode('utf-8').strip()
    s.close()
    logger.debug(f'response to ftp is {response}')
    return response


def bully(ip1, ip2):
    return int(ip1.split('.')[-1]) > int(ip2.split('.')[-1])

def not_self_discover(message):
    return message == FIND_COORDINATOR or message == FIND_OWNER

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
            return (k > start and k >= end) or (k <= start and k < end)
        
def reset_socket(s: socket.socket, target_ip, target_port) -> socket.socket:
    s.close()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((target_ip, target_port))
    return s

