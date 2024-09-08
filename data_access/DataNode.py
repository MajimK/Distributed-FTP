from utils.utils_functions import *
from utils.consts import DATABASE_PORT
from typing import Dict, List
from utils.operations import *
import threading
import socket
import time
from utils.file_system import *
import os
import shutil
import json

class DataNode:
    def __init__(self, ip: str, db_port: int = DATABASE_PORT) -> None:
        self.ip = ip
        self.id = getShaRepr(ip)
        self.db_port: int = db_port

        
        
        self.data: dict[dict[FileData]] = {}
        self.replicated_data: dict[dict[FileData]] = {}


        threading.Thread(target=self._recv, daemon=True).start()
        threading.Thread(target=self._prints, daemon=True).start()

    

    def save_data(self, is_replication: bool):
        """Save this instance to a JSON file."""
        file_name = 'data.json' if not is_replication else 'replicated_data.json'
        root_dir = os.path.join(ROOT, self.ip)
        file_path = os.path.join(root_dir, file_name)
        data = self.data if not is_replication else self.replicated_data
        
        with open(file_path, 'w') as f:
            json.dump(data, f)

    

    def handle_list_command(self, current_dir, client_socket:socket.socket):
        # with open('data_node.json', 'r') as f:
        #     json_data = json.load(f)

        if current_dir in self.data:
            dirs = self.data[current_dir]

            try:
                client_socket.send(f'220'.encode())
                response = client_socket.recv(1024).decode()

                if response.startswith('220'):
                    result = []
                    while True:
                        try:
                            result = list(dirs.values())
                            break
                        except Exception as e:
                            print(f"handle_list_command: {e}")
                    
                    #no me queda claro que es asi como quiero acceder a los recursos->analizar

                    client_socket.sendall('\n'.join(result).encode())   

            except Exception as e:
                print(f"handle_list_command: {e}")
        
        else:
            client_socket.send(f"404 Not Found".encode())

    def handle_mkd_command(self, completed_path: str, current_dir, file_data: FileData, client_socket: socket.socket, successor_ip = None):
        is_replication = successor_ip is None
        try:
            if not is_replication:
                self.data[completed_path] = {}
            else:
                print("REPLICA MKD")
                self.replicated_data[completed_path] = {}
                
            if not is_replication:
                client_socket.sendall(f'220'.encode())
                operation = f'{REPLICATE_MKD}'
                send_w_ack(operation, f'{completed_path},{current_dir},{file_data}', successor_ip, self.db_port)
            else:
                # mandar algún mensaje, controlar las respuestas mejor.
                pass
            
            self.save_data(is_replication)


        except Exception as e:
            print(f"handle_mkd_command: {e}")
            client_socket.send(f"403 Already exists".encode())

    def handle_stor_command(self, route: str, successor_ip:str, client_socket: socket.socket):
        try:
            path_without_root = route.split('/')[2:]
            path_without_root = '/'.join(path_without_root)
            path = os.path.normpath(ROOT+'/'+ self.ip + '/'+os.path.dirname(path_without_root))
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

    def handle_stor_filedata(self, current_directory, file_path, file_data, successor_ip:str=None):
        is_replication = successor_ip is None
        data = self.data if not is_replication else self.replicated_data

        if current_directory in data:
            print(f'_asign_filedata: EL DIRECTORIO {current_directory} EXISTE')
            dir = self.data[current_directory] if not is_replication else self.replicated_data[current_directory]
            dir[file_path] = file_data
        
        elif current_directory == ROOT: #it's first time
            print(f'_asign_filedata: EL DIRECTORIO {current_directory} NO EXISTE PERO ES ROOT')
            
            if not is_replication:
                self.data[current_directory] = {}
                dirs = self.data[current_directory]
                dirs[file_path] = file_data

            
            else: 
                print(f"ESTA REPLICANDO... entra con {current_directory} para replicar {file_path}")
                self.replicated_data[current_directory] = {}

                dirs = self.replicated_data[current_directory]
                dirs[file_path] = file_data


        else:
            return False
        if not is_replication:
                print(f'SUCCESSOR IP: {successor_ip}')
                operation = f'{REPLICATE_STORFILEDATA}'
                send_w_ack(operation, f'{current_directory},{file_path},{file_data}', successor_ip, self.db_port)
        self.save_data(is_replication)
        return True

    def handle_remove_directory(self, absolute_path, current_dir, successor_ip: str, client_socket: socket.socket):
        is_replication = successor_ip is None
        if not is_replication:
            if current_dir in self.data:
                dirs = self.data[current_dir]
                dirs.pop(absolute_path)
                self.data[current_dir] = dirs
                self.data.pop(absolute_path)
                operation = f'{REPLICATE_REMOVE_DIR}'
                print("LLAMANDO PARA REPLICAR ELIMINACION DE DIRECTORIO...")
                send_w_ack(operation, f'{absolute_path},{current_dir}', successor_ip, self.db_port)

                client_socket.sendall(f'220'.encode())
            else:
                client_socket.sendall(f'404 Not Found'.encode())
        else:
            if current_dir in self.replicated_data:
                dirs = self.replicated_data[current_dir]
                dirs.pop(absolute_path)
                self.replicated_data[current_dir] = dirs
                self.replicated_data.pop(absolute_path)
            else:
                client_socket.sendall(f'404 Not Found'.encode())

        self.save_data(is_replication) 

    def handle_rmd_command(self, route, successor_ip: str, client_socket: socket.socket):
        # route is the absolute path: current_dir/dir_to_remove
        if route in self.data:
            dirs = self.data[route]

            subdirs = list(dirs.items())

            directories = []
            files = []

            for subdir, file_data in subdirs:
                if file_data.is_dir():
                    directories.append(subdir)
                else:
                    files.append(subdir)

            response = '\n'.join(directories) + '\n' + END + '\n'.join(files)
            print(f"handle_rmd_command: LA RESPUESTA QUE SE ENVIARA DESDE EL DATANODE ES {response}")
            client_socket.sendall(f'220 {response}'.encode())
        
        else:
            client_socket.send(f"404 Not Found".encode())


    def migrate_data_to_new_node(self, new_node: 'DataNode', pred_node_id):
        # migrate directories
        for key in self.data.keys():
            if inbetween(key, pred_node_id, new_node.id):
                new_node.data[key] = self.data[key]
                self.data.pop(key)
        
        # migrate replicated directories
        for key in self.replicated_data.keys():
            new_node.replicated_data[key] = self.replicated_data[key]
            self.replicated_data.pop(key)

        # update self replicated data
        for key in new_node.data:
            self.replicated_data[key] = new_node.data[key]

        
        path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
        new_node_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'DATA')
       
        # clean new node folder before copying
        self.clean_folder(new_node_path) 

        # migrate files
        self.copy_folder_with_condition(path, new_node_path, new_node.id, pred_node_id)
        
        # clean new node replicated data folder before copying
        new_node_replicated_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'REPLICATED_DATA')
        self.clean_folder(new_node_replicated_path)
         
        # migrate replicated files
        replicated_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
        self.copy_files(replicated_path, new_node_replicated_path, True)
        
        # update replicated data of successor
        self.copy_files(new_node_path, replicated_path)

    def migrate_data_one_node(self, new_node: 'DataNode'):
        keys_to_transfer = [k for k in self.data.keys()]
        for key in keys_to_transfer:
            hash_key = getShaRepr(key)
            if inbetween(hash_key, self.id, new_node.id):    # if the key is in the interval (old_id, new_node.id]
                new_node.data[key] = self.data[key]
                del self.data[key]

        # share replicated data
        self.replicated_data = {}
        new_node.replicated_data = {}

        # update new node replicated data
        for key in self.data.keys():
            new_node.replicated_data[key] = self.data[key]

        # update self replicated data
        for key in new_node.data:
            self.replicated_data[key] = new_node.data[key]

        # clean new node folders before copying
        new_node_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'DATA')
        new_node_replicated_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'REPLICATED_DATA')
        rep_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')


        self.clean_folder(new_node_path)
        self.clean_folder(new_node_replicated_path)

        # migrate files
        source_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
        self.copy_folder_with_condition_one(source_path,new_node_path, new_node.id, self.id, rep_path, new_node_replicated_path)        
        


    def _recv(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.ip, self.db_port))
        # print(f"Error: {e} - {self.ip} - {self.db_port}")
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

        elif operation == STOR_FILEDATA:
            print("STOR_FILEDATA")
            current_dir = msg[1]
            file_path = msg[2]
            file_data = msg[3]
            successor_ip = msg[4]
            self.handle_stor_filedata(current_dir, file_path, file_data, successor_ip)

        elif operation == RMD:
            print("RMD_COMMAND")
            route = msg[1]
            successor_ip = msg[2]
            self.handle_rmd_command(route, successor_ip, conn)
        
        elif operation == REMOVE_DIR:
            path = msg[1]
            current_dir = msg[2]
            successor_ip = msg[3]
            self.handle_remove_directory(path, current_dir, successor_ip, conn)

        # replication section
        elif operation == REPLICATE_MKD:
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            route = data[0]
            current_dir = data[1]
            file_data = data[2]
            print("REPLICANDO MKD...")
            self.handle_mkd_command(route, current_dir, file_data, conn)
            conn.sendall(f'{OK}'.encode())

        elif operation == REPLICATE_STORFILEDATA:
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            current_directory = data[0]
            file_path = data[1]
            file_data = data[2]
            self.handle_stor_filedata(current_directory,file_path,file_data)
            conn.sendall(f'{OK}'.encode())

        elif operation == REPLICATE_REMOVE_DIR:
            print("REPLICATE_REMOVE_DIR")
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            abs_path = data[0]
            current_dir = data[1]
            self.handle_remove_directory(abs_path,current_dir, None, conn)
            print("REMOVIO CON REPLICATE_REMOVE_DIR")
            conn.sendall(f'{OK}'.encode())


        else:
            print("NADA DE NADA")


#------------------TEST------------------#
    def _prints(self):
        while True:
            time.sleep(10)
            print(f"CONTENIDO DE {self.ip}:")
            for d in self.data.keys():
                # print()
                print(f"DIRECTORIO: {d}: {self.data[d]}")
            
            for rd in self.replicated_data.keys():
                print(f'DIRECTORIO REPLICADO: {rd}: {self.replicated_data[rd]}')
            print("\n\n")



   #------------------UTILS------------------#
    def copy_files(self, source_path, dest_path, remove=False):
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            new_path = os.path.normpath(dest_path + '/' + file)
            shutil.copy2(file_path, new_path)
            if remove:
                os.remove(file_path)
    
    
    def copy_folder_with_condition(self, source_path, dest_path, dest_id, pred_id):
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            file_hash_path = getShaRepr(file_path)

            if inbetween(file_hash_path, pred_id, dest_id): 
                new_path = os.path.normpath(dest_path + '/' + file)
                shutil.copy2(file_path, new_path)
                os.remove(file_path)

    def copy_folder_with_condition_one(self, source_path, dest_path, dest_id, pred_id, rep_source_path, rep_dest_path):
        
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            file_hash_path = getShaRepr(file_path)

            if inbetween(file_hash_path, pred_id, dest_id): 
                new_path = os.path.normpath(dest_path + '/' + file)
                shutil.copy2(file_path, new_path)
                os.remove(file_path)
            else:
                rep_file_path = os.path.normpath(rep_source_path + '/' + file)
                new_path = os.path.normpath(rep_dest_path + '/' + file)
                shutil.copy2(rep_file_path, new_path)
                os.remove(rep_file_path)

    def clean_folder(self, path):
        # clean new node folder before copying
        for file in os.listdir(path):
            p = os.path.join(path, file)
            os.remove(p) 

    
    def create_its_folder(self):
        data_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
        os.makedirs(data_path, exist_ok=True)
        replicated_data_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
        os.makedirs(replicated_data_path, exist_ok=True)