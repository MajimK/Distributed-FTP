from utils.utils_functions import *
from utils.consts import DATABASE_PORT
from typing import Dict, List
from utils.operations import *
import threading
import socket
import time
from utils.file_system import *
import os
class DataNode:
    def __init__(self, ip: str, db_port: int = DATABASE_PORT) -> None:
        self.ip = ip
        self.db_port: int = db_port

        
        

        self.files: Dict[str, FileData] = {}
        self.replicated_files: Dict[str, FileData] = {}

        threading.Thread(target=self._recv, daemon=True).start()
        threading.Thread(target=self._prints, daemon=True).start()

    

    def owns_directory(self, directory_name: str):
        return directory_name in self.files


    def handle_mkd_command(self, directory_name: str, successor_ip: str, client_socket: socket.socket):
        
        try:
            self.make_directories(directory_name)
            client_socket.sendall(f'220'.encode())
            if successor_ip != self.ip:
                operation = f'{REPLICATE_MKD}'
                send_w_ack(operation, directory_name, successor_ip, self.db_port)
        except Exception as e:
            print(f"handle_mkd_command: {e}")
            client_socket.send(f"403 Already exists".encode())




    def make_directories(self, route: str, is_replicate = False):
        route = route.split('/')[2:]
        current_path = 'app'
        prev_directory = 'app'
        while route:
            directory = route.pop(0)
            current_path += '/' + directory
            if not is_replicate:
                if current_path not in self.files:
                    self.files[current_path] = FileData(directory,current_path,0,container=prev_directory)
            else:
                self.replicated_files[current_path] = FileData(directory, current_path, container=prev_directory)
            prev_directory = directory
        



        pass
    # def delete_directory(self, directory_name: str, successor_ip: str):
    #     deleted_direc = self.directories.pop(directory_name)
    #     operation = f'{REPLICATE_DELETE_DIRECTORY}'
    #     send_w_ack(operation, directory_name, successor_ip, self.db_port)

    
    # def add_file(self, directory: str, file_name:str, successor_ip):
    #     print(f"add_file: EL DIRECTORIO ES {directory} Y EL FILE ES {file_name}")
    #     self.directories[directory].append(file_name)
    #     operation = f'{REPLICATE_ADD_FILE}'
    #     msg = f'{directory},{file_name}'
    #     send_w_ack(operation, msg, successor_ip, self.db_port)

    def _recv(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.ip, self.db_port))
        s.listen(10)

        while True:
            conn, _ = s.accept()
            data = conn.recv(1024).decode()
            threading.Thread(target=self._data_receive, args=(conn, data)).start()

    def _data_receive(self, conn:socket.socket, msg:str):
        
        print(f"_data_receive: EL MENSAJE QUE ESTA LLEGANDO EN DATA_NODE ES {msg}")
        msg = msg.split(',')
        print(msg)
        try:
            operation = int(msg[0])
        except:
            operation = msg[0]

        if operation == MKD:
            # conn.sendall(f"{OK}".encode())
            # directory_name = conn.recv(1024).decode()
            route = msg[1]
            successor_ip = msg[2]
            self.handle_mkd_command(route, successor_ip, conn)
            # conn.sendall(f'{OK}'.encode())

        elif operation == REPLICATE_MKD:
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            route = data[0]
            self.make_directories(route,True)
            conn.sendall(f'{OK}'.encode())

        # elif operation == REPLICATE_DELETE_DIRECTORY:
        #     print(f"_data_receive: DELETE DIRECTORY ES LA OPERACION")
        #     conn.sendall(f"{OK}".encode())
        #     directory_name = conn.recv(1024).decode()
        #     deleted_directory = self.replicated_directories.pop(directory_name)
        #     if deleted_directory:
        #         conn.sendall(f'{OK}'.encode())
        #     else:
        #         print(f"EL DIRECTORIO {directory_name} NO EXISTE")


        # elif operation == REPLICATE_ADD_FILE:
        #     conn.sendall(f'{OK}'.encode())
        #     data = conn.recv(1024).decode().split(',')
        #     directory_name = data[0]
        #     file_name = data[1]
        #     self.replicated_directories[directory_name].append(file_name)
        #     conn.sendall(f'{OK}'.encode())


        else:
            print("NADA DE NADA")



    def _prints(self):
        while True:
            time.sleep(10)
            print(f"CONTENIDO DE {self.ip}:")
            for f in self.files.keys():
                print(f"DIRECTORIO: {f}: {self.files[f]}")
            
            print(f'REPLICATED DIRECTORIOS: {self.replicated_files.keys()}')
            print("\n\n")
