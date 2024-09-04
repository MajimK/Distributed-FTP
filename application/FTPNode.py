from dht.chord import ChordNode
from data_access.DataNode import DataNode
from utils.consts import *
import threading
from socket import socket
from utils.operations import *
from utils.utils_functions import *
import time
from datetime import datetime
import os
from utils.file_system import FileData
from communication.chord_node_reference import ChordNodeReference

class FTPNode(ChordNode):
    def __init__(self, ip: str, port: int = DEFAULT_PORT, ftp_port = FTP_PORT, m: int = 160):
        super().__init__(ip, port, m)
        
        self.ftp_port: int = ftp_port
        self.data_node: DataNode = DataNode(ip)


        threading.Thread(target=self.start_ftp_server, daemon=True).start()
        threading.Thread(target=self._test, daemon=True).start()

    def _handle_cwd_command(self):
        pass
    
    def _handle_dele_command(self, data: list):
        pass
       
    def _handle_list_command(self):
        pass
    
    def _handle_mkd_command(self, directory_name, client_socket: socket.socket, current_dir):
        new_path = os.path.normpath(os.path.join(current_dir, directory_name))
        directory_hash_name = getShaRepr(directory_name)
        owner:ChordNodeReference = self.find_succ(directory_hash_name)
        successor = owner.succ  # to replicating data
        file_data: FileData = FileData('drwxr-xr-x',os.path.basename(directory_name),0, datetime.now().strftime('%b %d %H:%M'))
        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner.ip, DATABASE_PORT))

        owner_socket.sendall(f'{MKD},{new_path},{current_dir},{file_data},{successor.ip}'.encode())
        response = owner_socket.recv(1024).decode().strip()

        if response.startswith('220'):
            print("220")
            owner_socket.close()
            if client_socket:
                client_socket.send(f'257 "{new_path}" created.\r\n'.encode())

        else:
            if client_socket:
                client_socket.send(b"550 Directory already exists.\r\n")


    def _handle_pasv_command(self):
        pass

    def _handle_port_command(self):
        pass

    def _handle_pwd_command(self):
        pass

    def _handle_retr_command(self):
        pass

    def _handle_rmd_command(self):
        pass

    def _handle_stor_command(self, file_name: str, client_socket: socket.socket, current_dir, data_transfer_socket: socket):
        file_path = os.path.join(current_dir,file_name)
        file_hash_name = getShaRepr(file_name)
        owner: ChordNodeReference = self.find_succ(file_hash_name)
        successor = owner.succ
        try:
            owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            owner_socket.connect((owner.ip, DATABASE_PORT))
            owner_socket.sendall(f'{STOR},{file_path},{successor.ip}'.encode())

            response = owner_socket.recv(1024).decode().strip()
            if response.startswith('220'):   
                client_socket.send(b"150 Opening binary mode data connection for file transfer.\r\n")

                owner_socket.send(b'220')

                while True:
                    data = data_transfer_socket.recv(4096)
                    owner_socket.sendall(data)
                    if not data:
                        break
                owner_socket.close()
                if client_socket:
                        client_socket.send(b"226 Transfer complete.\r\n")
        
            elif response.startswith('550'):
                if client_socket:
                    client_socket.send(b"550 File already exists.\r\n")
                
        except:
            pass
        
        pass
    def _handle_quit_command(self):
        pass

    def start_ftp_server(self):
        print("start_ftp_server: ENTRA")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.ftp_port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')
                print(f"start_ftp_server: DATA ES {data}")
                threading.Thread(target=self.receive_ftp_data, args=(conn, data)).start()


    def receive_ftp_data(self, conn: socket, data: list):
        current_dir = os.path.normpath('/app/database')
        operation = data[0]
        data_transfer_socket: socket = None
        print(f"receive_ftp_data: LA OPERACION ES {operation}")
        if operation == CWD:
            response = self._handle_stor_command(data[1:])  # route (implies mkd) or file

        elif operation == DELE:
            response = self._handle_dele_command(data[1:])
        
        elif operation == LIST:
            response = self._handle_list_command(data[1:])

        elif operation == MKD:
            response = self._handle_mkd_command(data[1], conn, current_dir)

        elif operation == PASV:
            response = self._handle_pasv_command()

        elif operation == PORT:
            response = self._handle_port_command()

        elif operation == PWD:
            response = self._handle_pwd_command()

        elif operation == RETR:
            response = self._handle_retr_command()

        elif operation == RMD:
            response = self._handle_rmd_command()

        elif operation == STOR:
            if data_transfer_socket:
                response = self._handle_stor_command(data[1],conn, current_dir, data_transfer_socket)
                data_transfer_socket.close()
                data_transfer_socket = None
        
        elif operation == SYST:
            conn.send(f'215 UNIX Type: L8\r\n'.encode())


        elif operation == QUIT:
            response = self._handle_quit_command()






        else:
            print("receive_ftp_data: NADA DE NADA FTP")

        if response:
            response = response.encode()
            conn.sendall(response)
        conn.close()

    

    
    def _test(self):
        time.sleep(8)
        print("ENTRA A _TEST!!!!")
        if self.ip == '172.17.0.2':
            print("Almacenando directorio...")
            r = self.ref.mkd("dir1").split(',')
            print(f"ESTO ES RESPONSE MILOCO: {r}")
            # r = self.ref.stor("perro.jpg").split(',')

            # r = self.ref.store_directory("dir2").split(',')
            # print(f'ESTO ES LA SEGUNDA RESPONSE MILOCO: {r}')

            # r = self.ref.add_file("dir2","dir2_file1")
            # print(f'ESTO ES EL RESPONSE DE ADD FILE MILOCO: {r}')
        
            # time.sleep(10)
            # print("Otras operaciones...")
            # r = self.ref.delete_directory("dir2").split(',')
            # print(f"ESTO ES EL RESPONSE DE DELETE DIRECTORY MILOCO: {r}")


        else:
            print("ENTRA SIENDO OTRO IP")



            

    

