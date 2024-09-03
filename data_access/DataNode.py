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

        
        
        self.data: dict[dict[FileData]] = {'/app/database': {}}
        self.replicated_data = {}


        threading.Thread(target=self._recv, daemon=True).start()
        threading.Thread(target=self._prints, daemon=True).start()

    

    def owns_directory(self, directory_name: str):
        return directory_name in self.files

    


        pass
    def handle_mkd_command(self, completed_path: str, current_dir, file_data: FileData, client_socket: socket.socket, successor_ip: str = None):
        try:
            if successor_ip:
                self.data[completed_path] = {}
                is_asigned = self._asign_filedata(current_dir, completed_path, file_data)
            else:
                self.replicated_data[completed_path] = {}
                is_asigned = self._asign_filedata(current_dir, completed_path, file_data, True)
            if is_asigned:
                if successor_ip:
                    client_socket.sendall(f'220'.encode())
                    if successor_ip != self.ip:
                        operation = f'{REPLICATE_MKD}'
                        send_w_ack(operation, f'{completed_path},{current_dir},{file_data}', successor_ip, self.db_port)
        except Exception as e:
            print(f"handle_mkd_command: {e}")
            client_socket.send(f"403 Already exists".encode())

    def handle_stor_command(self, route: str, successor_ip:str, client_socket: socket.socket):
        try:
            path_without_root = route.split('/')[2:]
            path_without_root = '/'.join(path_without_root)
            path = os.path.normpath("app/database/"+ self.ip + '/'+os.path.dirname(path_without_root))
            os.path.makedirs(path, exist_ok=True)
            complete_path = os.path.normpath(path+'/'+os.path.basename(path_without_root))

            with open(complete_path, "wb") as file:
                client_socket.send(f'220'.encode())
                response = client_socket.recv(1024)
                if response.startswith('220'):
                    while data:
                        data = client_socket.recv(1024)
                        if not data:
                            break

                        file.write(data)
        except Exception as e:
            print("Error:" + e)

        pass

    

    def _asign_filedata(self, directory, file_path, file_data, is_replication = False):

        if directory in self.data if not is_replication else self.replicated_data:
            print(f'_asign_filedata: EL DIRECTORIO {directory} EXISTE')
            dir = self.data[directory] if not is_replication else self.replicated_data[directory]

            dir[file_path] = file_data
            return True
        else:
            return False

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
            route = msg[1]
            current_dir = msg[2]
            file_data = msg[3]
            successor_ip = msg[4]
            self.handle_mkd_command(route, current_dir, file_data, conn, successor_ip)
        
        elif operation == STOR:
            route = msg[1]
            successor_ip = msg[2]
            self.handle_stor_command(route, successor_ip, conn)

        # replication section
        elif operation == REPLICATE_MKD:
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            route = data[0]
            current_dir = data[1]
            file_data = data[2]
            self.handle_mkd_command(route, current_dir, file_data, conn)
            conn.sendall(f'{OK}'.encode())



        else:
            print("NADA DE NADA")



    def _prints(self):
        while True:
            time.sleep(10)
            print(f"CONTENIDO DE {self.ip}:")
            for d in self.data.keys():
                # print()
                print(f"DIRECTORIO: {d}: {self.data[d]}")
            
            print(f'REPLICATED DIRECTORIOS: {self.replicated_data.keys()}')
            print("\n\n")

    # def make_directories(self, route: str, is_replicate = False):
    #     route = route.split('/')[2:]
    #     current_path = 'app'
    #     prev_directory = 'app'
    #     while route:
    #         directory = route.pop(0)
    #         current_path += '/' + directory
    #         if not is_replicate:
    #             if current_path not in self.files:
    #                 self.files[current_path] = {}
    #         else:
    #             self.replicated_files[current_path] = FileData(directory, current_path, container=prev_directory)
    #         prev_directory = directory
        
    # def store_file(self, current_directory, file_path, metadata):
    #     self.files[current_directory] = FileData(metadata['name'], file_path, metadata['size'], container=current_directory)

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