import os
import time
import random
import threading
from socket import socket
from threading import Lock
from utils.consts import *
from utils.operations import *
from datetime import datetime
from dht.chord import ChordNode
from utils.utils_functions import *
from utils.file_system import FileData
from data_access.DataNode import DataNode
from communication.chord_node_reference import ChordNodeReference



class FTPNode(ChordNode):
    data_transfer_socket: socket.socket = None
    current_dir: str = ROOT
    current_dir_lock = Lock()
    data_transfer_lock = Lock()
    def __init__(self, ip: str, port: int = DEFAULT_PORT, ftp_port = FTP_PORT, m: int = 160):
        super().__init__(ip, port, m)
        self.ftp_port: int = ftp_port
        self.data_node = DataNode(self.ip)

        threading.Thread(target=self.start_ftp_server, daemon=True).start()
        # threading.Thread(target=self._test, daemon=True).start()
    
    def _handle_dele_command(self, file_name, current_dir, client_socket: socket.socket):
        completed_path = os.path.normpath(os.path.join(current_dir, file_name))  # no contemplo si la ruta es absoluta
        path_hash = getShaRepr(completed_path)

        owner = self.find_succ(path_hash)
        successor = owner.succ
        predecessor = owner.pred

        coordinator_ip = self.elector.coordinator
        if coordinator_ip == None:
            time.sleep(4)
        
        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coordinator_socket.connect((coordinator_ip, COORDINATOR_PORT))
        coordinator_socket.sendall(f'{REQUEST},{DELE} -> {completed_path}'.encode('utf-8'))
        max_tries = 5
        coordinator_socket.settimeout(20)
        permission_response = None
        while permission_response != GRANT:
            try:
                permission_response  = coordinator_socket.recv(1024).decode('utf-8').strip()
                logger.debug('DELE -> Waiting response...')
            except TimeoutError:
                max_tries-=1
                if max_tries == 0:
                    logger.debug(f'MAX_TRIES = 0')
                    self._handle_dele_command(file_name, current_dir, client_socket)

        logger.debug('DELE -> GRANT')

        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner.ip, DATABASE_PORT))
        
        owner_socket.sendall(f'{DELE},{file_name},{current_dir},{successor.ip},{predecessor.ip}'.encode('utf-8'))
        response = owner_socket.recv(1024).decode('utf-8').strip()

        if response.startswith('220'):
            # manage the success
            owner_socket.close()


        
        pass

    def _handle_list_command(self, current_dir: str, client_socket: socket.socket):
        # client_ip, client_port = client_socket.getpeername()
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
            logger.debug("Waiting for resource...")
        
        coordinator_socket.close()
        logger.debug('LIST -> GRANT')
        logger.debug(f'ADDRESS -> {PROXY_IP}: {PROXY_PORT}')
        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner.ip, DATABASE_PORT))

        owner_socket.sendall(f'{LIST},{current_dir}'.encode('utf-8'))
        response = owner_socket.recv(1024).decode('utf-8').strip()

        logger.debug(f'LIST -> owner_socket response: {response}')
        if response.startswith('220'):
            owner_socket.send('220'.encode('utf-8'))   # esto sobra creo
            # secure_send(b'150 Here comes the directory listing\r\n', client_ip, client_port, 3)
            # client_socket = reset_socket(client_socket)
            client_socket.sendall(b'150 Here comes the directory listing.\r\n')
            data = ""
            logger.debug("LIST LLEGA HASTA AQUI")
            owner_socket.settimeout(5)
            while True: 
                try:
                    chunk = owner_socket.recv(4096).decode('utf-8')
                    logger.debug(f"CHUNK: {chunk}")
                    data+= chunk
                    if chunk == "":
                        break
                except TimeoutError:  # controlar mejor esto
                    break
            logger.debug("LIST -> SALE DEL WHILE TRUE")
            FTPNode.data_transfer_socket.sendall(data.encode('utf-8'))
            owner_socket.close()
            try:
                logger.debug(f'handle_list_command -> CLIENT_SOCKET: {client_socket}')
                client_socket.sendall(b"226 Directory send OK.\r\n")
                # secure_send(b"226 Directory send OK.\r\n", client_ip, client_port, 3)
            except BrokenPipeError:
                logger.debug("Connection was closed by the client. Broken pipe.")
                
            # client_socket.send(b"226 Directory send OK.\r\n")
            logger.debug("Transfer completed")  # funciona bien, pero client_socket se cierra o algo asi
            logger.debug(data)
        else:
            client_socket.send(b"550 Failed to list directory.\r\n")

        is_sent = secure_send(RELEASE, coordinator_ip, COORDINATOR_PORT, 5)
        if is_sent:
            logger.debug("LIST -> RELEASE SENT...")
        else:
            logger.debug("LIST -> RELEASE SENT FAILED")
            
    def _handle_mkd_command(self, directory_name, client_socket: socket.socket, current_dir):
        """This command causes the directory specified in the pathname
            to be created as a directory (if the pathname is absolute)
            or as a subdirectory of the current working directory (if
            the pathname is relative)
        """
        new_path = os.path.normpath(os.path.join(current_dir, directory_name))  # no contemplo si la ruta es absoluta
        path_hash_name = getShaRepr(new_path)
        logger.debug(f'HASH DE {new_path} ES {path_hash_name}')
        owner:ChordNodeReference = self.find_succ(path_hash_name)
        successor = owner.succ  # to replicating data
        predecessor = owner.pred
        file_data: FileData = FileData('drwxr-xr-x',os.path.basename(directory_name),0, datetime.now().strftime('%b %d %H:%M'))
        
        coordinator_ip = self.elector.coordinator
        if coordinator_ip == None:
            time.sleep(4)

        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coordinator_socket.connect((coordinator_ip, COORDINATOR_PORT))
        coordinator_socket.sendall(f'{REQUEST},{MKD} -> {new_path}'.encode('utf-8'))
        max_tries = 5
        coordinator_socket.settimeout(20)
        permission_response = None
        while permission_response != GRANT:
            try:
                permission_response = coordinator_socket.recv(1024).decode('utf-8').strip()
            except TimeoutError:
                max_tries-=1
                if max_tries == 0:
                    logger.debug(f'MAX_TRIES = 0')
                    self._handle_mkd_command(directory_name, client_socket, current_dir)
                
            logger.debug(f'IN WHILE -> permission_response: {permission_response}')
        coordinator_socket.close()
        logger.debug("MKD -> GRANT")

        
        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner.ip, DATABASE_PORT))
        
        owner_socket.sendall(f'{MKD},{new_path},{current_dir},{file_data},{successor.ip},{predecessor.ip}'.encode('utf-8'))
        response = owner_socket.recv(1024).decode('utf-8').strip()

        if response.startswith('220'):
            logger.debug("220")
            owner_socket.close()
            if self.stor_filedata(current_dir, new_path, file_data, owner.ip, successor.ip, predecessor.ip):
                logger.debug(f'257 "{new_path}" created.\r\n')
                if client_socket:
                    logger.debug("handle_mkd_command -> ENVIA AL CLIENTE")
                    client_socket.send(f'257 "{new_path}" created.\r\n'.encode('utf-8'))
            else:
                if client_socket:
                    client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

        else:
            if client_socket:
                client_socket.send(b"550 Directory already exists.\r\n")

        is_sent = secure_send(RELEASE, coordinator_ip, COORDINATOR_PORT, 5)
        if is_sent:
            logger.debug("MKD -> RELEASE SENT...")
        else:
            logger.debug("MKD -> RELEASE SENT FAILED")

    def _handle_pasv_command(self, client_socket:socket.socket,  port_range = (50000,50100)):
        for port in random.sample(range(*port_range), port_range[1] - port_range[0]):
            try:
                data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                data_socket.bind(('0.0.0.0', port))  # checks which are available
                break
            except OSError:
                continue
        
        data_socket.listen(1)
        port = data_socket.getsockname()[1]
        logger.debug(f'handle_pasv_command: PORT IS {port}')
        
        ip = '127,0,0,1'
        p1, p2 = divmod(port, 256)
        response = f'227 Entering Passive Mode ({ip},{p1},{p2}).\r\n'
        logger.debug(f'handle_pasv_command: RESPONSE IS {response}')
        client_socket.send(response.encode('utf-8'))

        data_socket.settimeout(5)  # Establecer un timeout de 5 segundos
        try:
            data_client, addr = data_socket.accept()
        except socket.timeout:
            logger.warning("Timeout while waiting for data connection.")
            data_client = None
        logger.debug('Accepting conecctions from data socket...')
        return data_client
    
    def _handle_port_command(self, addr):
        address_parts = addr.split(',') 
        ip_address = '.'.join(address_parts[:4]) 
        port = int(address_parts[4]) * 256 + int(address_parts[5]) 

        data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_socket.connect((ip_address, port))
        return data_socket

    def _handle_retr_command(self,current_dir,client_socket:socket.socket,file_name):
        file_hash_name = getShaRepr(file_name)
        

        owner: ChordNodeReference = self.find_succ(file_hash_name)
        try:
            owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            owner_socket.connect((owner.ip, DATABASE_PORT))
            owner_socket.sendall(f'{RETR},{file_name}'.encode())

            response = owner_socket.recv(1024).decode().strip()
            if response.startswith('225'):
                owner_socket.send(b'230')
                while True:
                    data = owner_socket.recv(1024)
                    if data.startswith('226'):
                        break
                    FTPNode.data_transfer_socket.send(data)
            client_socket.sendall(f'226 Transfer complete\r\n'.encode())
        except Exception as e:
            logger.debug(f'ERROR: {e}')
        
    def _handle_rmd_command(self, dir_path, current_dir, client_socket: socket.socket = None):
        """Causes the directory specified in the pathname
            to be removed as a directory (if the pathname is absolute)
            or as a subdirectory of the current working directory (if
            the pathname is relative)
        """

        absolute_path = os.path.join(current_dir, dir_path)
        logger.debug(f'LA RUTA COMPLETA DEL DIRECTORIO PARA BORRAR ES: {absolute_path}')
        directory_hash_name = getShaRepr(absolute_path)
        owner: ChordNodeReference = self.find_succ(directory_hash_name)
        successor = owner.succ
        predecessor = owner.pred
        
        coordinator_ip = self.elector.coordinator
        if coordinator_ip is None:
            time.sleep(10)

        coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        coordinator_socket.connect((coordinator_ip, COORDINATOR_PORT))
        coordinator_socket.sendall(f'{REQUEST},{RMD}'.encode('utf-8'))

        permission_response = None
        while permission_response != GRANT:
            permission_response = coordinator_socket.recv(1024).decode('utf-8').strip()
            logger.debug("Waiting for resource...")
        
        coordinator_socket.close()
        logger.debug('RMD -> GRANT')


        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner.ip, DATABASE_PORT))
        logger.debug(owner.ip)
        logger.debug(f"_handle_rmd_command: ENVIANDO RMD,{absolute_path},{current_dir} DESDE FTPNODE")
        owner_socket.sendall(f'{RMD},{absolute_path},{successor.ip},{predecessor.ip}'.encode('utf-8'))

        response = owner_socket.recv(1024).decode('utf-8').strip()
        logger.debug(f"RMD -> Response: {response}")
        if response.startswith('220'):
            owner_socket.close()

            lines = response[4:].split('\n')
            end_directories = lines.index(END)
            logger.debug(f'END_DIRECTORIES -> {end_directories}')
            directories = lines[:end_directories] if end_directories != 1 else []
            logger.debug(f'DIRECTORIES TO DELETE -> {directories}')
            files = lines[end_directories+1:] if len(lines)-end_directories > 1 else []
            logger.debug(f'FILES TO DELETE -> {files}')

            for dir in directories:
                self._handle_rmd_command(dir, os.path.normpath(os.path.dirname(dir)))
            
            for file in files:
                self._handle_dele_command(file, os.path.normpath(os.path.dirname(file)))
            logger.debug("SALE DEL CICLO DE BORRAR")
            if self.remove_directory(absolute_path, current_dir, owner.ip, successor.ip, predecessor.ip):
                logger.debug(f"handle_rmd_command -> {client_socket} XXX")
                client_socket.send(f'250 {absolute_path} deleted\r\n'.encode('utf-8'))
                logger.debug(f"handle_rmd_command -> SENT...")
            logger.debug("SALE DEL REMOVE DIRECTORY")
        else:
            if client_socket:
                client_socket.send(b"550 Directory do not exists.\r\n")
        logger.debug("SALE DE TODO")
        is_sent = secure_send(RELEASE, coordinator_ip, COORDINATOR_PORT, 5)
        logger.debug("ENVIA SECURE_SEND")
        if is_sent:
            logger.debug('RMD -> RELEASE SENT...')
        else:
            logger.debug('RMD -> RELEASE SENT FAILED')
        # no esta saliendo del RMD donde borra, ver eso.

    def _handle_stor_command(self, file_name: str, client_socket: socket.socket, current_dir):
        file_path = os.path.join(current_dir,file_name)
        file_hash_name = getShaRepr(file_name)
        owner: ChordNodeReference = self.find_succ(file_hash_name)
        successor = owner.succ
        predecessor = owner.pred

        try:
            coordinator_ip = self.elector.coordinator
            if coordinator_ip is None:
                time.sleep(10)

            coordinator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            coordinator_socket.connect((coordinator_ip, COORDINATOR_PORT))
            coordinator_socket.sendall(f'{REQUEST},{RMD}'.encode('utf-8'))

            permission_response = None
            while permission_response != GRANT:
                permission_response = coordinator_socket.recv(1024).decode('utf-8').strip()
                logger.debug("Waiting for resource...")
            
            coordinator_socket.close()
            logger.debug('STOR -> GRANT')


            owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            owner_socket.connect((owner.ip, DATABASE_PORT))
            owner_socket.sendall(f'{STOR},{file_name},{successor.ip},{predecessor.ip}'.encode('utf-8'))

            response = owner_socket.recv(1024).decode('utf-8').strip()
            if response.startswith('220'):   
                client_socket.send(b"150 Opening binary mode data connection for file transfer.\r\n")

                owner_socket.send(b'220')
                size = 0
                while True:
                    data = FTPNode.data_transfer_socket.recv(4096)
                    logger.debug(f"stor_command_FTP_NODE -> DATA: {data}")
                    owner_socket.sendall(data)
                    if not data:
                        break
                    size += len(data)
                    logger.debug(f'size -> {size}')

                owner_socket.close()
                file_data = FileData(f"-rw-r--r--", size, datetime.now().strftime('%b %d %H:%M'), {os.path.basename(file_name)})
                if self.stor_filedata(current_dir, file_path, file_data, owner.ip, successor.ip, predecessor.ip):
                    if client_socket:
                        client_socket.send(b"226 Transfer complete.\r\n")
                else:
                    if client_socket:
                        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")
                
        except:
            pass

        is_sent = secure_send(RELEASE, coordinator_ip, COORDINATOR_PORT, 5)
        if is_sent:
            logger.debug('STOR -> RELEASE SENT...')
        else:
            logger.debug('STOR -> RELEASE SENT FAILED')
        
        pass

    def start_ftp_server(self):
        logger.debug("start_ftp_server: ENTRA")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.ftp_port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                logger.debug(f'WELCOME <-(: x-x-x :)-> {addr}')

                data = conn.recv(1024).decode('utf-8').split()
                logger.debug(f"start_ftp_server: DATA ES {data}")
                threading.Thread(target=self.receive_ftp_data, args=(conn, data,)).start()

    def receive_ftp_data(self, conn: socket, data: list):
        operation = data[0]
        logger.debug(f"LA OPERACION ES {operation}")
        with FTPNode.current_dir_lock:
            logger.debug(f'CURRENT_DIR ES {FTPNode.current_dir}')
        logger.debug(f'Data transfer lock has value {FTPNode.data_transfer_lock}')
        logger.debug(f'Data transfer socket has value {FTPNode.data_transfer_socket}')
        with FTPNode.data_transfer_lock:
            logger.debug(f'DATA_TRANSFER_SOCKET ES {FTPNode.data_transfer_socket}')
        logger.debug(f'Pasó del lock de data_transfer...')

        try:
            if operation == CWD:
                new_path = os.path.normpath(data[1].strip())
                with FTPNode.current_dir_lock:
                    if FTPNode.current_dir != os.path.normpath("/app/database") or new_path != "..":
                        FTPNode.current_dir = os.path.normpath(os.path.join(FTPNode.current_dir, new_path))
                conn.send(b'250 Directory successfully changed.\r\n')

            elif operation == DELE:
                self._handle_dele_command(data[1:])

            
            elif operation == LIST:
                with FTPNode.data_transfer_lock:
                    if FTPNode.data_transfer_socket:
                        logger.debug('VA A LISTAR...')
                        self._handle_list_command(FTPNode.current_dir, conn)
                        FTPNode.data_transfer_socket.close()
                        FTPNode.data_transfer_socket = None
                    else:
                        logger.debug('NO VA A LISTAR!!!!!')
                

            elif operation == MKD:
                logger.debug(f"ENTRA A MKD CON EL DIRECTORIO {data[1]}")
                self._handle_mkd_command(data[1], conn, FTPNode.current_dir)

    
            elif operation == PASV:
                with FTPNode.data_transfer_lock:
                    FTPNode.data_transfer_socket = self._handle_pasv_command(conn)
                    logger.debug(f'data_transfer_socket: {FTPNode.data_transfer_socket}')

            elif operation == PORT:
                addr = data[1]
                with FTPNode.data_transfer_lock:
                    FTPNode.data_transfer_socket = self._handle_port_command(addr)
                conn.send(b'200 PORT command successful.\r\n')

            elif operation == PWD:
                conn.send(f'257 "{FTPNode.current_dir}" is the current directory.\r\n'.encode('utf-8'))
                pass

            elif operation == RETR:
                file_name  = data[1]
                with FTPNode.data_transfer_lock:
                    if FTPNode.data_transfer_socket:
                        self._handle_retr_command(FTPNode.current_dir, conn,file_name )
                        FTPNode.data_transfer_socket.close()
                        FTPNode.data_transfer_socket = None

            elif operation == RMD:
                logger.debug("ENTRA A RMD")
                dir_path = data[1]
                self._handle_rmd_command(dir_path,FTPNode.current_dir, conn)

            elif operation == STOR:
                logger.debug('ENTRA A STOR!!!')
                file_name = data[1]
                with FTPNode.data_transfer_lock:
                    if FTPNode.data_transfer_socket:
                        self._handle_stor_command(file_name,conn, FTPNode.current_dir)                        
                        FTPNode.data_transfer_socket.close()
                        FTPNode.data_transfer_socket = None
            
            else:
                logger.debug("FTPNode.py -> Command not found!")

        
        except ConnectionAbortedError:
            logger.debug("Connection aborted by peer")
        except ConnectionResetError:
            logger.debug("Connection reset by peer")
        finally:
            pass
            

    
    #-----------------AUXILIAR METHODS REGION-----------------#
    # hice este metodo porque cuando voy a hacer el STOR se cosas del FileData despues de que llamé al metodo STOR en el DataNode
    def stor_filedata(self, current_dir, file_path, filedata: FileData, owner_ip, successor_ip, predecessor_ip):
            logger.debug('stor_filedata')

            owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            owner_socket.connect((owner_ip, DATABASE_PORT))

            owner_socket.send(f'{STOR_FILEDATA},{current_dir},{file_path},{filedata},{successor_ip},{predecessor_ip}'.encode('utf-8'))

            response = owner_socket.recv(1024).decode('utf-8').strip()
            logger.debug(f'stor_filedata: RESPONSE: {response}')
            if response.startswith('220'):
                logger.debug(f'stor_filedata: 220')
                owner_socket.close()
                logger.debug(f'stor_filedata: RETURN TRUE')

                return True
            else:
                logger.debug(f'stor_filedata: RETURN FALSE')

                return False

    def remove_directory(self, absolute_path, current_dir, owner_ip, successor_ip, predecessor_ip):
        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner_ip, DATABASE_PORT))

        owner_socket.send(f'{REMOVE_DIR},{absolute_path},{current_dir},{successor_ip},{predecessor_ip}'.encode('utf-8'))

        response = owner_socket.recv(1024).decode('utf-8').strip()   
        if response.startswith('220'):
            owner_socket.close()
            return True
        else: return False

    def remove_filedata(self, file_name, current_dir, owner_ip, successor_ip, predecessor_ip):
        owner_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        owner_socket.connect((owner_ip, DATABASE_PORT))

        owner_socket.send(f'{DELE_FILEDATA},{file_name},{current_dir},{successor_ip},{predecessor_ip}'.encode('utf-8'))

        response = owner_socket.recv(1024).decode('utf-8').strip()
        if response.startswith('220'):
            owner_socket.close()
            return True
        else: return False

