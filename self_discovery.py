from consts import *
import threading
import socket
from utils import send_by_broadcast
from operations import DISCOVER, ENTRY_POINT
import time

class SelfDiscovery:
    def __init__(self, ip: str, port: int = PORT) -> None:
        self.ip = ip
        self.port = port

        self.target_ip = None
        threading.Thread(target=self._recv, daemon=True).start()


    def find(self) -> str: 
        """Sends a DISCOVERY operation by broadcast with its ip and port

        Returns:
            str: Returns the target ip
        """
        send_by_broadcast(f'{DISCOVER},{self.ip},{self.port}')
        print(f'find: MENSAJE ENVIADO POR BROADCAST')
        while not self.target_ip:
            # print(f'find: WAITING...')

            time.sleep(0.25)

        print("find: TIENE UN IP AL QUE CONECTARSE")
        return self.target_ip
    

    def _recv(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(5)
            print(f"_recv: ESPERANDO RESPUESTA POR {self.ip}:{self.port}")

            while True:
                conn, addr = s.accept()

                print(f'_rev: MENSAJE DE {addr[0]}')

                if addr[0] == self.ip:
                    continue

                data = conn.recv(1024).decode().split(',')
                option = int(data[0])

                if option == ENTRY_POINT:
                    print(f"_recv: {self.ip} RECIBIO RESPUESTA DE {data[1]}")
                    self.target_ip = data[1]
                    conn.close()
                    s.close()
                    break




