from utils.consts import COORDINATOR_PORT
import socket
import threading
from utils.consts import commands, REQUEST, GRANT, RELEASE, OK
from threading import Lock
import time
class Coordinator():
    def __init__(self, ip, port = COORDINATOR_PORT) -> None:
        self.ip = ip
        self.coord_port = port
        self.processes = []
        self.token_owner: socket.socket = None
        self.token_owner_lock = Lock()
        self.processes_lock = Lock()
        
        threading.Thread(target=self._recv).start()
        threading.Thread(target=self._see_queue).start()

    def _see_queue(self):
        while True:
            with self.token_owner_lock:
                with self.processes_lock:
                    print(f'EN LA COLA ESTáN -> {self.processes}')
                    if self.token_owner is None and self.processes:
                        self.token_owner = self.processes.pop(0)
                        self.token_owner.send(GRANT.encode('utf-8'))   # creo que lo que pasa es aqui
                        print("GRANT sent from _see_queue...")
            time.sleep(1)


    
    def _recv(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try: 
            s.bind((self.ip, self.coord_port))
        except Exception as e:
            print(f"COORDINATOR: Error in _recv -> {e}")
        s.listen(10)

        while True:
            conn, _ = s.accept()
            message = conn.recv(1024).decode('utf-8')
            print(f'MESSAGE RECEIVED -> {message}')
            threading.Thread(target=self._handle, args=(conn, message)).start()


    def _handle(self, client_socket: socket.socket, message: str):
        print(f"COORDINATOR: Received message -> {message} -> {self.token_owner}")

        try:
            command = message.split(",")[0]
            # operation = message.split(",")[1] # no se si necesite esto
            # access to resource is requested
            if command == REQUEST:
                with self.token_owner_lock:
                    with self.processes_lock:
                        if self.token_owner is None and not self.processes:
                            self.token_owner = client_socket
                                
                            client_socket.send(GRANT.encode('utf-8'))  # puede ser que se maree aqui
                            print("handle: ENVIO GRANT")
                        else:
                            self.processes.append(client_socket)
                            print("_handle: ENTRA A ELSE DE REQUEST")
                        pass
            # resource is released
            elif command == RELEASE:
                print("handle: ENTRO A ESTE RELEASE")
                with self.token_owner_lock:
                    self.token_owner = None
                    print(f'handle: TOKEN_OWNER: {self.token_owner}')
                    client_socket.send(f'{OK}'.encode('utf-8'))
                    print("handle: HIZO NONE A TOKEN_OWNER...")

            else: print("COORDINATOR: _handle -> Wrong command")
        except Exception as e:
            print(f"COORDINATOR: _handle -> Wrong format --> {e}")

        pass


