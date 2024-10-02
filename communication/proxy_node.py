from communication.self_discovery import SelfDiscovery
from utils.consts import PROXY_IP, PROXY_PORT, FTP_PORT, OK
from utils.utils_functions import logger, reset_socket
import threading
import socket

def handle_client(client_socket: socket.socket):
    sd = SelfDiscovery(PROXY_IP)
    sd.find()
    target_ftp = sd.target_ip

    if target_ftp is None:
        client_socket.send(b"421 No available nodes.\r\n")
        client_socket.close()
        return
    try:
        ftp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ftp_socket.connect((target_ftp, FTP_PORT))
        ftp_socket.settimeout(10)
        while True:
            try:
                message = client_socket.recv(1024).decode().strip()
                if message == '':
                    break
                else:
                    while True:
                        try:
                            print(f'handle_client: MESSAGE TO FTP -> {message}')
                            ftp_socket.sendall(message.encode('utf-8'))
                        
                            print(f'hadle_client: MESSAGE TO FTP SENT...')
                            ftp_response= ftp_socket.recv(4096).decode('utf-8').strip()

                            print(f'handle_client: FTP_RESPONSE -> {ftp_response}')
                            client_socket.sendall(ftp_response.encode('utf-8'))
                            break
                        except TimeoutError as e:
                            ftp_socket = reset_socket(ftp_socket, target_ftp, FTP_PORT)
                            continue

            except Exception as e:
                print(f'Error in handle_client -> proxy_node: {e}')
    except:
        print(f'Error in handle_client -> proxy_node: {e}')

    finally:
        client_socket.close()
        ftp_socket.close()


def start_proxy_server():
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy_socket.bind((PROXY_IP, PROXY_PORT))
    proxy_socket.listen(5)
    print(f"Proxy FTP listening on {PROXY_IP}:{PROXY_PORT}")

    while True:
        client_socket, addr = proxy_socket.accept()
        print(f"Cliente conectado desde {addr}")

        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()    


def call_proxy():
    start_proxy_server()