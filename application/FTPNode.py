from dht.chord import ChordNode
from data_access.DataNode import DataNode
from utils.consts import *
import threading
from socket import socket
from utils.operations import *
from utils.utils_functions import *
import time

class FTPNode(ChordNode):
    def __init__(self, ip: str, port: int = PORT, ftp_port = FTP_PORT, m: int = 160):
        super().__init__(ip, port, m)
        
        self.ftp_port: int = ftp_port
        self.data_node: DataNode = DataNode(ip)


        threading.Thread(target=self.start_ftp_server, daemon=True).start()
        threading.Thread(target=self._test, daemon=True).start()

    def _handle_store_item(self, data: list):
        direc_name = data[0]
        direc_name_hash = getShaRepr(direc_name)
        print(f'handle_insert_directory: NOMBRE HASH: {direc_name_hash}')
        owner = self.find_succ(direc_name_hash)
        if owner.id == self.id:
            if self.data_node.owns_directory(direc_name):
                print("ERROR,Key already exists")
                return "ERROR,Key already exists"
            else:
                self.data_node.store_directory(direc_name, self.succ.ip)
                print("OK,Data inserted")
                return "OK,Data inserted"
        else:
            response = owner.store_directory(direc_name)
            return response
    
    def _handle_delete_directory(self, data: list):
        direc_name = data[0]
        direc_name_hash = getShaRepr(direc_name)
        print(f'handle_delete_directory: NOMBRE HASH: {direc_name_hash}')
        owner = self.find_succ(direc_name_hash)
        if owner.id == self.id:
            if self.data_node.owns_directory(direc_name):
                self.data_node.delete_directory(direc_name, self.succ.ip)
                print("OK,Data deleted")
                return "OK,Data deleted"
            else:
                print("ERROR,Key doesn't exist")
                return "ERROR,Key doesn't exist"
        else:
            response = owner.delete_directory(direc_name)
            return response

    def _handle_add_file(self, data: list):
        direc_name = data[0]
        file_name = data[1]
        direc_name_hash = getShaRepr(direc_name)
        print(f'handle_add_file: NOMBRE HASH: {direc_name_hash}')

        owner = self.find_succ(direc_name_hash)
        if owner.id == self.id:
            if self.data_node.owns_directory(direc_name):
                self.data_node.add_file(directory=direc_name,file_name=file_name,successor_ip=self.succ.ip)
                print("_handle_add_file: OK, ARCHIVO AGREGADO")
                return "OK,File added"
            else:
                print("_handle_add_file: ERROR, EL DIRECTORIO NO EXISTE")
                "ERROR,Directory name doesn't exist"
        else:
            response = owner.add_file(direc_name, file_name)
            return response


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
                threading.Thread(target=self.receive_ftp_data, args=(conn, addr, data)).start()


    def receive_ftp_data(self, conn: socket, addr,  data: list):
        
        operation = int(data[0])
        print(f"receive_ftp_data: LA OPERACION ES {operation}")
        if operation == STOR:
            response = self._handle_insert_directory(data[1:])

        elif operation == DELE:
            response = self._handle_delete_directory(data[1:])
        
        elif operation == ADD_FILE:
            response = self._handle_add_file(data[1:])




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
            r = self.ref.store_directory("dir1").split(',')
            print(f"ESTO ES RESPONSE MILOCO: {r}")

            r = self.ref.store_directory("dir2").split(',')
            print(f'ESTO ES LA SEGUNDA RESPONSE MILOCO: {r}')

            r = self.ref.add_file("dir2","dir2_file1")
            print(f'ESTO ES EL RESPONSE DE ADD FILE MILOCO: {r}')
        
            time.sleep(10)
            print("Otras operaciones...")
            r = self.ref.delete_directory("dir2").split(',')
            print(f"ESTO ES EL RESPONSE DE DELETE DIRECTORY MILOCO: {r}")


        else:
            print("ENTRA SIENDO OTRO IP")



            

    

