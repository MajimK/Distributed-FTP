import socket
import os
import json
import threading
import random

class FTP_Handler():
    def __init__(self,root) -> None:
        self.root = root
        self.current_dir = root
        self.data_socket = None
        self.data_connection = None

    def handle_list(self, client_socket):
        file_list=os.listdir(self.current_dir)
        response = '\n'.join(file_list) + '\r\n'
        if self.data_connection:
            self.data_connection.sendall(response.encode())
            self.data_connection.close()
            self.data_socket.close()
            client_socket.sendall(b'226 Transfer complete\r\n')
        else:
            client_socket.sendall(b'425 No data connection\r\n')

    def handle_upload(self, client_socket ,file_name):
        if self.data_connection:
            filepath=os.path.join(self.current_dir,file_name)
            with open(filepath,'wb') as document:
                while True:
                    data = self.data_connection.recv(1024)
                    if not data:
                        break
                    document.write(data)
            self.data_connection.close()
            self.data_socket.close()
            client_socket.sendall(b'226 Transfer complete\r\n')
        else:
            client_socket.sendall(b'425 No data connection\r\n')

    def handle_download(self, client_socket,file_name):
        if self.data_connection:
            filepath = filepath=os.path.join(self.current_dir,file_name)
            if os.path.isfile(filepath):
                with open(filepath, 'rb') as document:
                    while True:
                        data = document.read(1024)
                        if not data:
                            break
                        self.data_connection.sendall(data)
                self.data_connection.close()
                self.data_socket.close()
                client_socket.sendall(b'226 Transfer complete\r\n')
            else:
                client_socket.sendall(b'550 File not found\r\n')
        else:
            client_socket.sendall(b'425 No data connection\r\n')


    def handle_del(self,client_socket, file_name):
        file_path = os.path.join(self.current_dir,file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
            client_socket.sendall(b'250 File deleted successfully\r\n')
        else:
            client_socket.sendall(b'550 File not found\r\n')

    def handle_change_dir(self,client_socket, new_dir):
        new_path = os.path.join(self.current_dir, new_dir)
        if os.path.isdir(new_path):
            os.chdir(new_path)
            self.current_dir= new_path
            client_socket.sendall(b'250 Directory successfully changed\r\n')
        else:
            client_socket.sendall(b'550 Directory not found\r\n')

    def handle_working_dir(self,client_socket):
        response = f'257 "{self.current_dir}"is the current directory\r\n'
        client_socket.sendall(response.encode())

    def handle_create(self,client_socket, dir_name):
        path = os.path.join(self.current_dir,dir_name)
        try:
            os.mkdir(path)
            client_socket.sendall(b'257 Directory created\r\n')
        except FileExistsError:
            client_socket.sendall(b'550 Directory already exists\r\n')
        except FileNotFoundError:
            client_socket.sendall(b'550 Directory not found\r\n')

    def handle_port(self,client_socket, args):
        ip_parts = args.split(',')
        ip_address = '.'.join(ip_parts)
        port = int(ip_address[-2]) * 256 + int(ip_address[-1])
        
        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data_socket.connect((ip_address, port))
        self.data_connection=self.data_socket
        
        response = '200 PORT command successful\r\n'
        client_socket.sendall(response.encode())


    def handle_pasv(self,client_socket):
        port=random.randint(49152,65535)
        self.data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data_socket.bind(('localhost', port))  
        self.data_socket.listen(1)
        
        ip, port = self.data_socket.getsockname()
        ip = ip.replace('.', ',')
        p1, p2 = divmod(port, 256)
        response = f'227 Entering Passive Mode ({ip},{p1},{p2})\r\n'
        client_socket.sendall(response.encode())
        

        self.data_connection, _ = self.data_socket.accept()


class FTP_Server():
    def __init__(self,root,commands,users):
        self.root=root
        self.commands=commands
        self.users=users
        self.ftp_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.authenticated = {}
        self.current_user = {}

    def handle_client(self,client_socket):
        client_socket.sendall(b'220 FTP Server Ready\r\n')
        handler=FTP_Handler(self.root)
        username=''
        authenticated = False

        while True:
            data = client_socket.recv(1024).decode().strip()
            if not data:
                break

            command, *args = data.split()
            command = command.upper()
            print(command,args)

            if command == 'USER':
                username=args[0]
                if username in self.users:
                    self.current_user[client_socket] = username
                    client_socket.sendall(b'331 Password required for ' + username.encode() + b'\r\n')
                else:
                    client_socket.sendall(b'530 Invalid username\r\n')
            elif command == 'PASS':
                passw = args[0]
                if client_socket in self.current_user and self.users[self.current_user[client_socket]] == passw:
                    self.authenticated[client_socket] = True
                    client_socket.sendall(b'230 User logged in\r\n')
                else:
                    client_socket.sendall(b'530 Login incorrect\r\n')

            elif command == 'QUIT':
                client_socket.sendall(b'221 Goodbye\r\n')
                break
            elif command == 'LIST':
                if self.authenticated.get(client_socket, False):
                    handler.handle_list(client_socket)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'RETR':
                if self.authenticated.get(client_socket, False):
                    filename = args[0]
                    handler.handle_download(client_socket, filename)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'STOR':
                if self.authenticated.get(client_socket, False):
                    filename = args[0]
                    handler.handle_upload(client_socket, filename)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'DELE':
                if self.authenticated.get(client_socket, False):
                    filename = args[0]
                    handler.handle_del(client_socket, filename)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'CWD':
                if self.authenticated.get(client_socket, False):
                    directory = args[0]
                    handler.handle_change_dir(client_socket, directory)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'MKD':
                if self.authenticated.get(client_socket, False):
                    directory = args[0]
                    handler.handle_create(client_socket, directory)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'PWD':
                if self.authenticated.get(client_socket, False):
                    handler.handle_working_dir(client_socket)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'PORT':
                if self.authenticated.get(client_socket, False):
                    handler.handle_port(client_socket, args)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'PASV':
                if self.authenticated.get(client_socket, False):
                    handler.handle_pasv(client_socket)
                else:
                    client_socket.sendall(b'530 Please login with USER and PASS\r\n')
            elif command == 'TYPE':
                if args[0].upper() == 'I':
                    response = '200 Type set to I (binary)\r\n'
                    client_socket.sendall(response.encode())
                else:
                    client_socket.sendall(b'504 Command not implemented for that parameter\r\n')
            else:
                client_socket.sendall(b'500 Command not supported\r\n')

ROOT='ftp_folder'
DIR= os.path.join(os.getcwd(),ROOT)
COMMANDS = ['USER', 'PASS', 'QUIT', 'LIST', 'RETR','STOR', 'DELE', 'CWD', 'PWD', 'MKD', 'RMD'] 
VALID_USERS = {'Kevin':'Kevin','Jan Carlos':'Jan Carlos'}


server_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
server=FTP_Server(DIR,COMMANDS,VALID_USERS)

server_address=('0.0.0.0',21)
server.ftp_socket.bind(server_address)
server.ftp_socket.listen(5)
print('El servidor FTP está escuchando en el puerto 21')



while True:
    client_socket, client_address = server.ftp_socket.accept()

    print('Conexión desde', client_address)

    client_handler = threading.Thread(target=server.handle_client, args=(client_socket,))
    client_handler.start()

