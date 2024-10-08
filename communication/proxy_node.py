from communication.self_discovery import SelfDiscovery
from utils.consts import *
from utils.utils_functions import logger, reset_socket
import threading
import socket

# target_ip = None
def handle_client(client_socket: socket.socket, target_ftp):
    # global target_ip
    # target_ip = target_ftp
    try:
        while True:
            
            command = client_socket.recv(1024).decode('utf-8').strip()
            logger.debug(f'proxy_ftp -> command: {command}')
            # if command == '':
            #     continue

            if command == FEAT:
                features = '211 Features \r\n'
                for cmd in commands:
                    features += f'{cmd}\r\n'
                features+= '211 End\r\n'
                client_socket.sendall(features.encode('utf-8'))

            
            elif command.startswith(SYST):
                client_socket.send(f'215 UNIX Type: L8\r\n'.encode('utf-8'))

            elif command.startswith(TYPE_A):
                client_socket.sendall(b'200 Switching to ASCII mode.\r\n')

            elif command.startswith(TYPE_I):
                client_socket.sendall(b'200 Switching to Binary mode.\r\n')

            elif command.startswith(USER):
                client_socket.sendall(b'230 User logged in, proceed.\r\n')

            elif command.startswith(AUTH_TLS) or command.startswith(AUTH_SSL):
                client_socket.sendall(b'502 Command not implemented.\r\n')

            elif command.startswith(QUIT):
                client_socket.sendall(b'221 Goodbye\r\n')

            else: # controlar que sean los otros comandos ;)
                operation = command.split()[0]
                    
                logger.debug(f'proxy_ftp -> no operation {operation}')
                if operation in commands:
                    ftp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    ftp_socket.connect((target_ftp, FTP_PORT))
                    ftp_socket.settimeout(20)
                    ftp_socket.sendall(command.encode('utf-8'))
                    try:
                        while True:
                            response = ftp_socket.recv(1024).decode('utf-8')
                            client_socket.sendall(response.encode('utf-8'))
                            logger.debug(f"proxy_ftp -> Response: {response}")
                            if response[0] == '2' or response[0] == '5':
                                ftp_socket.close()
                                break
                    except TimeoutError:
                        logger.debug("TIMEOUT ERROR!!!!")
                        ftp_socket.close()
                else:
                    client_socket.send(b'500 Syntax error, command unrecognized.\r\n')
    
    except ConnectionAbortedError:
        logger.debug("Connection aborted by peer")
    except ConnectionResetError:
        logger.debug("Connection reset by peer")
    except ConnectionRefusedError:
        logger.debug("Connection to FTP Node is refused")
        # target_ip = None
        

        pass
    finally:
        logger.debug("-----------------------------")
        client_socket.close()



def start_proxy_server():
    # global target_ip
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy_socket.bind((PROXY_IP, PROXY_PORT))
    print(f'PROXY IP IS {PROXY_IP}')
    proxy_socket.listen(5)
    logger.debug(f"Proxy FTP listening on {PROXY_IP}:{PROXY_PORT}")

    sd = SelfDiscovery(PROXY_IP)
    sd.find()
    target_ftp = sd.target_ip
    # target_ip = target_ftp
    # tengo que descubrir siempre
    if target_ftp is None:
        client_socket.send(b"421 No available nodes.\r\n")
        client_socket.close()
        return
        
    while True:
        client_socket, addr = proxy_socket.accept()
        client_socket.sendall(b'220 Welcome to the FTP server!\r\n')
        logger.debug('PASO DE SEND') 
        # if target_ip is None:
        #     sd.find()
        #     target_ftp = sd.target_ip

        client_handler = threading.Thread(target=handle_client, args=(client_socket,target_ftp))
        client_handler.start()    


def call_proxy():
    start_proxy_server()