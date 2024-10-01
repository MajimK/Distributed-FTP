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

# Hay que manejar en estos metodos todo el tiempo la posibilidad de que no se hayan hecho, o sea, que devuelvan 550


class FTPNode(ChordNode):
    def __init__(self, ip: str, port: int = DEFAULT_PORT, ftp_port = FTP_PORT, m: int = 160):
        super().__init__(ip, port, m)
        
        self.ftp_port: int = ftp_port
        self.data_node = DataNode(self.ip)
        
        threading.Thread(target=self.start_ftp_server, daemon=True).start()
        threading.Thread(target=self._test, daemon=True).start()
    
    def _handle_dele_command(self, data: list):

        pass
       
    def _handle_list_command(self, current_dir: str, client_socket: socket.socket, data_transfer_socket: socket.socket):
        current_dir_hash = getShaRepr(current_dir)
        owner = self.find_succ(current_dir_hash)
        
        
        coordinator_ip = self.elector.coordinator
        if coordinator_ip is None:
            time.sleep(10)

        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coordinator_socket.connect((coordinator_ip, COORDINATOR_PORT))
        coordinator_socket.sendall(f'{REQUEST},{LIST}'.encode('utf-8'))

        permission_response = None
        while permission_response != GRANT:
            permission_response = coordinator_socket.recv(1024).decode('utf-8').strip()
            print("Waiting for resource...")
        
        coordinator_socket.close()
        print('LIST -> GRANT')
        
        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner.ip, DATABASE_PORT))

        owner_socket.sendall(f'{LIST},{current_dir}'.encode())
        response = owner_socket.recv(1024).decode().strip()

        if response.startswith('220'):
            owner_socket.send('220'.encode())   # esto sobra creo
            # client_socket.send(b'150 Here comes the directory listing\r\n')
            data = ""
            print("LIST LLEGA HASTA AQUI")
            owner_socket.settimeout(5)
            while True: 
                try:
                    chunk = owner_socket.recv(4096).decode('utf-8')
                    print(f"CHUNK: {chunk}")
                    data+= chunk
                    if chunk == "":
                        break
                except TimeoutError:  # controlar mejor esto
                    break
            print("LIST -> SALE DEL WHILE TRUE")
            # data_transfer_socket.sendall(data.encode('utf-8'))
            owner_socket.close()
            try:
                print(f'handle_list_command -> CLIENT_SOCKET: {client_socket}')
                client_socket.send(b"226 Directory send OK.\r\n")
            except BrokenPipeError:
                print("Connection was closed by the client. Broken pipe.")
                
            # client_socket.send(b"226 Directory send OK.\r\n")
            print("Transfer complete")  # funciona bien, pero client_socket se cierra o algo asi
            print(data)
        else:
            client_socket.send(b"550 Failed to list directory.\r\n")

        is_sent = secure_send(RELEASE, coordinator_ip, COORDINATOR_PORT, 5)
        if is_sent:
            print("LIST -> RELEASE SENT...")
        else:
            print("LIST -> NO SE MANDO RELEASE")
            
    
    def _handle_mkd_command(self, directory_name, client_socket: socket.socket, current_dir):
        """This command causes the directory specified in the pathname
            to be created as a directory (if the pathname is absolute)
            or as a subdirectory of the current working directory (if
            the pathname is relative)
        """
        new_path = os.path.normpath(os.path.join(current_dir, directory_name))  # no contemplo si la ruta es absoluta
        path_hash_name = getShaRepr(new_path)
        print(f'HASH DE {new_path} ES {path_hash_name}')
        owner:ChordNodeReference = self.find_succ(path_hash_name)
        successor = owner.succ  # to replicating data
        file_data: FileData = FileData('drwxr-xr-x',os.path.basename(directory_name),0, datetime.now().strftime('%b %d %H:%M'))
        
        coordinator_ip = self.elector.coordinator
        if coordinator_ip == None:
            time.sleep(10)

        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coordinator_socket.connect((coordinator_ip, COORDINATOR_PORT))
        coordinator_socket.sendall(f'{REQUEST},{MKD} -> {new_path}'.encode('utf-8'))
        max_tries = 5
        coordinator_socket.settimeout(20)
        permission_response = None
        while permission_response != GRANT:
            try:
                permission_response = coordinator_socket.recv(1024).decode().strip()
            except socket.timeout:
                max_tries-=1
                if max_tries == 0:
                    print(f'MAX_TRIES = 0')
                    self._handle_mkd_command(directory_name, client_socket, current_dir)
                
            print(f'IN WHILE -> permission_response: {permission_response}')
        coordinator_socket.close()
        print("MKD -> GRANT")

        
        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner.ip, DATABASE_PORT))
        
        owner_socket.sendall(f'{MKD},{new_path},{current_dir},{file_data},{successor.ip}'.encode())
        response = owner_socket.recv(1024).decode().strip()

        if response.startswith('220'):
            print("220")
            owner_socket.close()
            if self.stor_filedata(current_dir, new_path, file_data, successor.ip, owner.ip):
                print(f'257 "{new_path}" created.\r\n')
                if client_socket:
                    print("handle_mkd_command -> ENVIA AL CLIENTE")
                    client_socket.send(f'257 "{new_path}" created.\r\n'.encode())
            else:
                if client_socket:
                    client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

        else:
            if client_socket:
                client_socket.send(b"550 Directory already exists.\r\n")

        is_sent = secure_send(RELEASE, coordinator_ip, COORDINATOR_PORT, 5)
        if is_sent:
            print("MKD -> RELEASE SENT...")
        else:
            print("NO SE MANDO EL RELEASE EN MKD")



    def _handle_pasv_command(self):
        pass

    def _handle_port_command(self):

        pass

    def _handle_retr_command(self):
        pass

    def _handle_rmd_command(self, dir_path, current_dir, client_socket: socket.socket = None):
        """Causes the directory specified in the pathname
            to be removed as a directory (if the pathname is absolute)
            or as a subdirectory of the current working directory (if
            the pathname is relative)
        """

        absolute_path = os.path.join(current_dir, dir_path)
        print(f'LA RUTA COMPLETA DEL DIRECTORIO PARA BORRAR ES: {absolute_path}')
        directory_hash_name = getShaRepr(absolute_path)
        owner: ChordNodeReference = self.find_succ(directory_hash_name)
        successor = owner.succ
        
        coordinator_ip = self.elector.coordinator
        if coordinator_ip is None:
            time.sleep(10)

        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coordinator_socket.connect((coordinator_ip, COORDINATOR_PORT))
        coordinator_socket.sendall(f'{REQUEST},{RMD}'.encode('utf-8'))

        permission_response = None
        while permission_response != GRANT:
            permission_response = coordinator_socket.recv(1024).decode('utf-8').strip()
            print("Waiting for resource...")
        
        coordinator_socket.close()
        print('RMD -> GRANT')


        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner.ip, DATABASE_PORT))
        print(owner.ip)
        print(f"_handle_rmd_command: ENVIANDO RMD,{absolute_path},{current_dir} DESDE FTPNODE")
        owner_socket.sendall(f'{RMD},{absolute_path},{successor.ip}'.encode())

        response = owner_socket.recv(1024).decode().strip()
        print(f"RMD -> Response: {response}")
        if response.startswith('220'):
            owner_socket.close()

            lines = response[4:].split('\n')
            end_directories = lines.index(END)
            print(f'END_DIRECTORIES -> {end_directories}')
            directories = lines[:end_directories] if end_directories != 1 else []
            print(f'DIRECTORIES TO DELETE -> {directories}')
            files = lines[end_directories+1:] if len(lines)-end_directories > 1 else []
            print(f'FILES TO DELETE -> {files}')

            for dir in directories:
                self._handle_rmd_command(dir, os.path.normpath(os.path.dirname(dir)))
            
            for file in files:
                self._handle_dele_command(file, os.path.normpath(os.path.dirname(file)))
            print("SALE DEL CICLO DE BORRAR")
            if self.remove_directory(absolute_path, current_dir, successor.ip, owner.ip):
                print(f"handle_rmd_command -> {client_socket} XXX")
                client_socket.send(f'250 {absolute_path} deleted\r\n'.encode())
                print(f"handle_rmd_command -> SENT...")
            print("SALE DEL REMOVE DIRECTORY")
        else:
            if client_socket:
                client_socket.send(b"550 Directory do not exists.\r\n")
        print("SALE DE TODO")
        is_sent = secure_send(RELEASE, coordinator_ip, COORDINATOR_PORT, 5)
        print("ENVIA SECURE_SEND")
        if is_sent:
            print('RMD -> RELEASE SENT...')
        else:
            print('RMD -> NO SE MANDO NI CARAJO')
        # no esta saliendo del RMD donde borra, ver eso.

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
                size = 0
                while True:
                    data = data_transfer_socket.recv(4096)
                    owner_socket.sendall(data)
                    if not data:
                        break
                    size += len(data)

                owner_socket.close()
                response = client_socket.recv(1024).strip()
                if response.startswith('220'):
                    file_data = FileData(f"-rw-r--r--", size, datetime.now().strftime('%b %d %H:%M'), {os.path.basename(file_name)})
                    if self.stor_filedata(current_dir, file_path, file_data, successor.ip, owner.ip):
                        if client_socket:
                            client_socket.send(b"226 Transfer complete.\r\n")
                    else:
                        if client_socket:
                            client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")
                 
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
        try:
            if operation == CWD:
                new_path = os.path.normpath(data[1].strip())
                if current_dir != os.path.normpath("/app/database") or new_path != "..":
                    current_dir = os.path.normpath(os.path.join(current_dir, new_path))

            elif operation == DELE:
                self._handle_dele_command(data[1:])

            elif operation == FEAT:
                features = '211 Features \r\n'
                for cmd in commands:
                    features += f'{cmd}\r\n'
                features+= '211 End\r\n'
                conn.sendall(features.encode())
            
            elif operation == LIST:
                # if data_transfer_socket is None:
                #     data_transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                #     data_transfer_socket.connect((self.ip, DATA_TRANSFER_PORT))
                self._handle_list_command(current_dir, conn, data_transfer_socket)
                # if data_transfer_socket:
                #     data_transfer_socket.close()
                #     data_transfer_socket = None
                

            elif operation == MKD:
                print(f"ENTRA A MKD CON EL DIRECTORIO {data[1]}")
                self._handle_mkd_command(data[1], conn, current_dir)

    
            elif operation == PASV:
                self._handle_pasv_command()

            elif operation == PORT:
                self._handle_port_command()

            elif operation == PWD:
                conn.send(f'257 "{current_dir}" is the current directory.\r\n'.encode())

            elif operation == RETR:
                self._handle_retr_command()

            elif operation == RMD:
                print("ENTRA A RMD")
                dir_path = data[1]
                self._handle_rmd_command(dir_path,current_dir, conn)

            elif operation == STOR:
                print('ENTRA A STOR!!!')
                file_name = data[1]
                data_transfer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                data_transfer_socket.connect((self.ip, DATA_TRANSFER_PORT))
                if data_transfer_socket:
                    self._handle_stor_command(file_name,conn, current_dir, data_transfer_socket)
                    
                    data_transfer_socket.close()
                    data_transfer_socket = None
            
            elif operation == SYST:
                conn.send(f'215 UNIX Type: L8\r\n'.encode())

            elif operation == TYPE_A:
                conn.sendall(b'200 Switching to ASCII mode.\r\n')

            elif operation == TYPE_I:
                conn.sendall(b'200 Switching to Binary mode.\r\n')

            elif operation == USER:
                conn.sendall(b'230 User logged in, proceed.\r\n')

            elif operation == AUTH_TLS or operation == AUTH_SSL:
                conn.sendall(b'500 Command not implemented.\r\n')


            elif operation == QUIT:
                conn.sendall(b'221 Goodbye\r\n')






            else:
                print("receive_ftp_data: NADA DE NADA FTP")

        
        except ConnectionAbortedError:
            print("Connection aborted by peer")
        except ConnectionResetError:
            print("Connection reset by peer")
        finally:
            if data_transfer_socket:
                data_transfer_socket.close()
            # conn.close()

    


    #-----------------AUXILIAR METHODS REGION-----------------#
    # hice este metodo porque cuando voy a hacer el STOR se cosas del FileData despues de que llamé al metodo STOR en el DataNode
    def stor_filedata(self, current_dir, file_path, filedata: FileData, successor_ip, owner_ip):
            print('stor_filedata')
        # try:
            owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            owner_socket.connect((owner_ip, DATABASE_PORT))
            owner_socket.send(f'{STOR_FILEDATA},{current_dir},{file_path},{filedata},{successor_ip}'.encode())

            response = owner_socket.recv(1024).decode().strip()
            print(f'stor_filedata: RESPONSE: {response}')
            if response.startswith('220'):
                print(f'stor_filedata: 220')
                owner_socket.close()
                print(f'stor_filedata: RETURN TRUE')

                return True
            else:
                print(f'stor_filedata: RETURN FALSE')

                return False
            
        # except Exception as e:
        #     print(f"stor_filedata: {e}")
        #     pass



    def remove_directory(self, absolute_path, current_dir, successor_ip, owner_ip):
        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner_ip, DATABASE_PORT))
        owner_socket.send(f'{REMOVE_DIR},{absolute_path},{current_dir},{successor_ip}'.encode())

        response = owner_socket.recv(1024).decode().strip()   # aqui debo en algun momento controlar que el dir no este disponible
        if response.startswith('220'):
            owner_socket.close()
            return True
        else: return False








    #-----------------TEST METHODS REGION-----------------#
    # implementar el CWD
    def _test(self):
        time.sleep(20)
        print("ENTRA A _TEST!!!!")
        if self.ip == '172.17.0.2':
            print("Almacenando directorio...")
            self.ref.mkd("dir1")
            print(f"SALIO DE MKD1")
            self.ref.mkd("dir2")
            print(f"SALIO DE MKD2")
            # r = self.ref.mkd("dir1/dir3").split(',')
            self.ref.rmd("dir2")
            print(f"SALIO DE RMD")
            # time.sleep(10)
            # print("VA PARA EL MKD DIR3")
            time.sleep(5)
            self.ref.mkd("dir3")
            print(f"SALIO DE MKD3")
            time.sleep(2)
            self.ref.list()
            print('SALIO DEL LIST')
            

            # print(f"RESPONSE DE RMD: {r}")
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



            

    

