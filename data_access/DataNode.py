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
import errno
import fcntl
from threading import Lock

class DataNode:
    def __init__(self, ip: str, db_port: int = DATABASE_PORT) -> None:
        self.ip = ip
        self.id = getShaRepr(ip)
        self.db_port: int = db_port
        self.data_lock = Lock()
        self.jsons_lock = Lock()

        
        
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

        logger.debug(f"SAVE_DATA -> {self.data} AND IS_REPLICATION: {is_replication} AND DATA: {data}")

        with open(file_path, 'w') as f:
            json.dump(data, f)

    def handle_dele_command(self, file_name, current_dir, client_socket: socket.socket, successor_ip = None, predecessor_ip = None):
        is_replication = successor_ip is None

        path = os.path.normpath(ROOT+'/'+ self.ip + '/'  + 'DATA' + '/' + file_name) if not is_replication else os.path.normpath(ROOT+'/'+ self.ip + '/'  + 'REPLICATED_DATA' + '/' + file_name)
        logger.debug(f'path -> {path}')
        try:
            if os.path.isfile(path):
                os.remove(path)

                if not is_replication:
                    client_socket.send('220'.encode('utf-8'))
                    logger.debug('Replicating dele...')
                    operation = f'{REPLICATE_DELE}'
                    send_replication_message(operation, f'{file_name},{current_dir}', self.db_port, successor_ip, predecessor_ip)
                    logger.debug('replication dele message sent...')
            else:
                if not is_replication:
                    client_socket.send('404'.encode('utf-8'))
        except Exception as e:
            logger.debug(f'error {e}')
            if not is_replication:
                client_socket.send(f'Error in handle_dele_command: {e}'.encode('utf-8'))                

    def handle_list_command(self, current_dir, client_socket:socket.socket):

        logger.debug(f'handle_list_DATA_NODE -> CURRENT_DIR: {current_dir}')
        
        with self.jsons_lock:
            self.load_jsons()
        # '/app/database/Manage/vitals.txt': '-rw-r--r-- 1 0 0 Oct 09 04:18 vitals.txt 208'
        if current_dir in self.data:
            dirs = self.data[current_dir]
            logger.debug(f'handle_list_command -> DIRS: {dirs}')

            try:
                client_socket.send(f'220'.encode())
                response = client_socket.recv(1024).decode()
                logger.debug(f"handle_list_command -> RESPONSE: {response}")

                if response.startswith('220'):
                    result = []
                    while True:
                        try:
                            result = list(dirs.values())
                            break
                        except Exception as e:
                            logger.debug(f"handle_list_command: {e}")
                    
                    response = '\n'.join(result)
                    logger.debug(f'Data_Node response: {response}')
                    if response == '':
                        response = END
                    client_socket.sendall(response.encode('utf-8'))   

            except Exception as e:
                logger.debug(f"handle_list_command: {e}")
        
        else:
            client_socket.send(f"404 Not Found".encode('utf-8'))

    def handle_mkd_command(self, completed_path: str, current_dir, file_data: FileData, client_socket: socket.socket, successor_ip = None, predecessor_ip = None):
        is_replication = successor_ip is None
        try:
            with self.jsons_lock:
                self.load_jsons(is_replication)
            if not is_replication:
                self.data[completed_path] = {}
                with self.jsons_lock:
                    self.save_data(is_replication)
                logger.debug(f'SELF.DATA -> {self.data}')
            else:
                logger.debug("REPLICA MKD")
                self.replicated_data[completed_path] = {}
                logger.debug(f'replicated data after replicate: {self.replicated_data}')
                with self.jsons_lock:
                    self.save_data(is_replication)
                
            if not is_replication:
                client_socket.sendall(f'220'.encode())
                operation = f'{REPLICATE_MKD}'
                send_replication_message(operation, f'{completed_path},{current_dir},{file_data}', self.db_port, successor_ip, predecessor_ip)

     
           


        except Exception as e:
            logger.debug(f"handle_mkd_command: {e}")
            client_socket.send(f"403 Already exists".encode())

    def handle_stor_command(self, file_name: str, client_socket: socket.socket, successor_ip:str, predecessor_ip: str):
        try:
            path = os.path.normpath(ROOT+'/'+ self.ip + '/'  + 'DATA' + '/' + file_name)
            # os.makedirs(path, exist_ok=True)

            with open(path, "wb") as file:
                client_socket.send(f'220'.encode())
                response = client_socket.recv(1024).decode('utf-8')
                if response.startswith('220'):
                    
                    while True:
                        data = client_socket.recv(1024)
                        logger.debug(f'handle_stor_command: DATA -> {data}')
                        if not data:
                            break
                        file.write(data)

            operation = f'{REPLICATE_STOR}'
            send_replication_message(operation, path, self.db_port, successor_ip, predecessor_ip)

        except Exception as e:
            logger.debug(f"Error: {e}")

        pass

    def handle_stor_filedata(self, current_directory, file_path, file_data, client_socket: socket.socket = None, successor_ip:str=None, predecessor_ip = None):
        logger.debug('start stor_filedata (DN)')
        is_replication = successor_ip is None
        with self.jsons_lock:
            self.load_jsons(is_replication)
        data = self.data if not is_replication else self.replicated_data
        logger.debug(f'Data is {data} and is_replication is {is_replication}')
        if current_directory in data:
            logger.debug(f'_asign_filedata: EL DIRECTORIO {current_directory} EXISTE')
            dir = self.data[current_directory] if not is_replication else self.replicated_data[current_directory]
            dir[file_path] = file_data
            with self.jsons_lock:
                self.save_data(is_replication)
            if not is_replication:
                client_socket.send('220'.encode())

            
            # if not is_replication:
            #     self.data[current_directory] = {}
            #     dirs = self.data[current_directory]
            #     dirs[file_path] = file_data
            #     client_socket.send('220'.encode())
            
            # else: 
            #     logger.debug(f"ESTA REPLICANDO... entra con {current_directory} para replicar {file_path}")
            #     self.replicated_data[current_directory] = {}

            #     dirs = self.replicated_data[current_directory]
            #     dirs[file_path] = file_data


        else:
            if not is_replication:
                client_socket.send('404'.encode())
        if not is_replication:
                logger.debug(f'SUCCESSOR IP: {successor_ip}')
                operation = f'{REPLICATE_STORFILEDATA}'
                send_replication_message(operation, f'{current_directory},{file_path},{file_data}', self.db_port, successor_ip, predecessor_ip)
        

    def handle_remove_directory(self, absolute_path, current_dir, client_socket: socket.socket, successor_ip: str = None, predecessor_ip = None):
        is_replication = successor_ip is None
        with self.jsons_lock:
            self.load_jsons(is_replication)
        if not is_replication:
            if absolute_path in self.data:
                self.data.pop(absolute_path)
                logger.debug(f'Dirs after pop is {self.data}')
                # self.data[current_dir] = dirs
                # self.data.pop(absolute_path)
                logger.debug(f"AFTER REMOVE -> {self.data}")
                operation = f'{REPLICATE_REMOVE_DIR}'
                logger.debug("LLAMANDO PARA REPLICAR ELIMINACION DE DIRECTORIO...")
                client_socket.sendall(f'220'.encode())
                with self.jsons_lock:   
                    self.save_data(is_replication) 

                send_replication_message(operation, f'{absolute_path},{current_dir}', self.db_port, successor_ip, predecessor_ip)

            else:
                client_socket.sendall(f'404 Not Found'.encode())
        else:
                if absolute_path in self.replicated_data:
                    self.replicated_data.pop(absolute_path)
                    with self.jsons_lock:   
                        self.save_data(is_replication) 

    def handle_remove_file(self, file_name: str, current_dir, client_socket, successor_ip = None, predecessor_ip = None):
        is_replication = successor_ip is None
        with self.jsons_lock:
            self.load_jsons(is_replication)
        if file_name.__contains__('/'):
            absolute_path = file_name
        else: absolute_path = current_dir+'/'+file_name
        logger.debug(f'data: {self.data}')
        logger.debug(f'replicated_data: {self.replicated_data} and is_replication {is_replication}')
        logger.debug(f'absolute_path: {absolute_path}')
        logger.debug(f'current_dir: {current_dir}')

        
        if not is_replication:
            if current_dir in self.data:
                dirs = self.data[current_dir]
                if absolute_path in dirs:
                    dirs.pop(absolute_path)
                    self.data[current_dir] = dirs  
                    logger.debug(f'self.data[current_dir] = {self.data[current_dir]}')
                    operation = f'{REPLICATE_DELEFILEDATA}'
                    with self.jsons_lock:
                        self.save_data(is_replication) 
                    send_replication_message(operation, f'{file_name},{current_dir}', self.db_port, successor_ip, predecessor_ip)

                client_socket.sendall(f'220'.encode('utf-8'))
            else:
                client_socket.sendall(f'404 Not Found'.encode('utf-8'))
        else:
            if current_dir in self.replicated_data:
                dirs = self.replicated_data[current_dir]
                logger.debug(f'Dirs before removing is {dirs}')
                if absolute_path in dirs:
                    logger.debug('absolute path is in dirs')
                    dirs.pop(absolute_path)
                    self.replicated_data[current_dir] = dirs
                    logger.debug(f'Replicated after removing is {self.replicated_data}')
                    with self.jsons_lock:
                        self.save_data(is_replication) 

    
    def handle_retr_command(self, file_name :str, client_socket: socket.socket):
        abs_path = os.path.normpath(ROOT +'/'+ self.ip + '/DATA/'+ file_name )
        logger.debug(f'path: {abs_path} and is file: {os.path.isfile(abs_path)}')
        # logger.debug(os.path.curdir)
        # if os.path.isfile(os.path.basename(abs_path)):
        client_socket.send(b'225')
        with open(abs_path, 'rb') as file:
            data = b''
            response = client_socket.recv(1024).decode().strip()
            if response.startswith('230'):
                while True:
                    data = file.read(1024)
                    logger.debug(f'Data from file {data}')
                    if not data:
                        break
                    client_socket.sendall(data)
        client_socket.sendall(b'226 Transfer complete\r\n')

        # else:
        #     client_socket.sendall(b'550 File not found\r\n')

    def handle_rmd_command(self, route, client_socket: socket.socket):
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
            logger.debug(f"handle_rmd_command: LA RESPUESTA QUE SE ENVIARA DESDE EL DATANODE ES {response}")
            client_socket.sendall(f'220 {response}'.encode())
        
        else:
            client_socket.send(f"404 Not Found".encode())

    def handle_replicate_stor(self, file_path: str):
            new_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA' + '/')
            shutil.copy2(file_path, new_path)


    

    def _recv(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try: 
            s.bind((self.ip, self.db_port))
        except OSError as e:
            if e.errno == errno.EADDRINUSE:
                logger.debug(f"Still using {self.ip} - {self.db_port}...")
            else:
                logger.debug(f"Error {e} -> {self.ip} - {self.db_port}")
        except Exception as e:
            logger.debug(f"Error in _recv -> {e}")
        s.listen(10)

        while True:
            conn, _ = s.accept()
            data = conn.recv(1024).decode()
            threading.Thread(target=self._data_receive, args=(conn, data)).start()

    def _data_receive(self, conn:socket.socket, msg:str):
        
        logger.debug(f"_data_receive: EL MENSAJE QUE ESTA LLEGANDO EN DATA_NODE ES {msg}")
        msg = msg.split(',')
        logger.debug(msg)
        try:
            operation = int(msg[0])
        except:
            operation = msg[0]

        if operation == DELE:
            file_name = msg[1]
            current_dir = msg[2]
            successor_ip = msg[3]
            predecessor_ip = msg[4]
            logger.debug(f'successor_ip -> {successor_ip}')
            logger.debug(f'predecessor_ip -> {predecessor_ip}')

            self.handle_dele_command(file_name, current_dir, conn, successor_ip, predecessor_ip)

        elif operation == DELE_FILEDATA:
            file_name = msg[1]
            current_dir = msg[2]
            successor_ip = msg[3]
            predecessor_ip = msg[4]
            # abs_path = current_dir+'/'+file_name
            self.handle_remove_file(file_name,current_dir, conn, successor_ip, predecessor_ip)

        elif operation == LIST:
            current_dir = msg[1]
            self.handle_list_command(current_dir, conn)

        elif operation == MKD:
            route = msg[1]
            current_dir = msg[2]
            file_data = msg[3]
            successor_ip = msg[4]
            predecessor_ip = msg[5]
            self.handle_mkd_command(route, current_dir, file_data, conn, successor_ip, predecessor_ip)
        
        elif operation == STOR:
            route = msg[1]
            successor_ip = msg[2]
            predecessor_ip = msg[3]
            self.handle_stor_command(route, conn, successor_ip, predecessor_ip)

        elif operation == STOR_FILEDATA:
            logger.debug("STOR_FILEDATA")
            current_dir = msg[1]
            file_path = msg[2]
            file_data = msg[3]
            successor_ip = msg[4]
            predecessor_ip = msg[5]
            self.handle_stor_filedata(current_dir, file_path, file_data, conn, successor_ip, predecessor_ip)

        elif operation == RMD:
            logger.debug("RMD_COMMAND")
            route = msg[1]
            successor_ip = msg[2]
            self.handle_rmd_command(route, conn)  # really sends the directories and files

        elif operation == RETR:
            logger.debug('RETR_COMMAND')
            file_name = msg[1]
            self.handle_retr_command(file_name, conn)
        
        elif operation == REMOVE_DIR:
            path = msg[1]
            current_dir = msg[2]
            successor_ip = msg[3]
            predecessor_ip = msg[4]
            self.handle_remove_directory(path, current_dir, conn, successor_ip, predecessor_ip)

        # replication section
        elif operation == REPLICATE_DELE:
            logger.debug('REPLICATE_DELE')
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            logger.debug(data)
            file_name = data[0]
            current_dir = data[1]
            self.handle_dele_command(file_name,current_dir,conn)
            conn.sendall(f'{OK}'.encode())

        elif operation == REPLICATE_DELEFILEDATA:
            logger.debug('REPLICATE_DELEFILEDATA')
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            abs_path = data[0]
            current_dir = data[1]
            self.handle_remove_file(abs_path, current_dir, conn)
            conn.sendall(f'{OK}'.encode())

        elif operation == REPLICATE_MKD:
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            route = data[0]
            current_dir = data[1]
            file_data = data[2]
            logger.debug("REPLICANDO MKD...")
            self.handle_mkd_command(route, current_dir, file_data, conn)
            conn.sendall(f'{OK}'.encode())

        elif operation == REPLICATE_REMOVE_DIR:
            logger.debug("REPLICATE_REMOVE_DIR")
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            abs_path = data[0]
            current_dir = data[1]
            self.handle_remove_directory(abs_path,current_dir, conn)
            logger.debug("REMOVIO CON REPLICATE_REMOVE_DIR")
            conn.sendall(f'{OK}'.encode())

        elif operation == REPLICATE_STOR:
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            file_path = data[0]
            self.handle_replicate_stor(file_path)
            conn.sendall(OK.encode())

        elif operation == REPLICATE_STORFILEDATA:
            conn.sendall(f'{OK}'.encode())
            data = conn.recv(1024).decode().split(',')
            current_directory = data[0]
            file_path = data[1]
            file_data = data[2]
            self.handle_stor_filedata(current_directory, file_path, file_data)
            conn.sendall(f'{OK}'.encode())


        else:
            logger.debug("NADA DE NADA")


#------------------TEST------------------#
    def _prints(self):
        while True:
            time.sleep(10)
            logger.debug(f"CONTENIDO DE {self.ip}:")
            for d in self.data.keys():
                # logger.debug()
                logger.debug(f"DIRECTORIO: {d}: {self.data[d]}")
            
            for rd in self.replicated_data.keys():
                logger.debug(f'DIRECTORIO REPLICADO: {rd}: {self.replicated_data[rd]}')
            logger.debug("\n\n")



   #------------------UTILS------------------#


    def load_jsons(self, is_replication: bool = None):
        replicated_data_file = os.path.normpath(ROOT + '/' + self.ip + '/' + 'replicated_data.json')
        data_file = os.path.normpath(ROOT + '/' + self.ip + '/' + 'data.json')
  
        if is_replication == False or is_replication is None:
            try:
                with open(data_file, 'r') as f:
                    self.data = json.load(f)
                    logger.debug(f"LOAD_JSON -> DATA: {self.data}")
            except Exception as e:
                logger.debug(f"LOAD_JSON -> {e} -> data_file: {data_file}")

        if is_replication == True or is_replication is None:
            try:
                with open(replicated_data_file, 'r') as f:
                    self.replicated_data = json.load(f)
                    logger.debug(f'LOAD_JSON -> REPLICATED: {self.replicated_data}')
            except Exception as e:
                logger.debug(f"LOAD_JSON -> {e} -> rep_file: {replicated_data_file}")


    