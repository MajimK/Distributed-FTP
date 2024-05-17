import socket
import os
import json
import threading

ROOT='ftp_folder'
DIR= os.path.join(os.getcwd(),ROOT)
COMMANDS = ['USER', 'PASS', 'QUIT', 'LIST', 'RETR','STOR', 'DELE', 'CWD', 'PWD', 'MKD', 'RMD'] 
VALID_USERS = {'Kevin':'Kevin','Jan Carlos':'Jan Carlos'}

def handle_list(client_socket):
    file_list=os.listdir(DIR)
    response = '\n'.join(file_list) + '\r\n'
    client_socket.sendall(response.encode())

def handle_upload(client_socket ,file_name):
    filepath=os.path.join(DIR,file_name)
    with open(filepath,'wb') as document:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            document.write(data)
    client_socket.sendall(b'226 Transfer complete\r\n')


def handle_download(client_socket,file_name):
    filepath = filepath=os.path.join(DIR,file_name)
    if os.path.isfile(filepath):
        with open(filepath, 'rb') as document:
            data=document.read()
        client_socket.sendall(data)
    else:
        client_socket.sendall(b'550 File not found\r\n')


def handle_del(client_socket, file_name):
    file_path = os.path.join(DIR,file_name)
    if os.path.isfile(file_path):
        os.remove(file_path)
        client_socket.sendall(b'250 File deleted successfully\r\n')
    else:
        client_socket.sendall(b'550 File not found\r\n')

def handle_change_dir(client_socket, new_dir):
    new_path = os.path.join(DIR, new_dir)
    if os.path.isdir(new_path):
        os.chdir(new_path)
        DIR= new_path
        client_socket.sendall(b'250 Directory successfully changed\r\n')
    else:
        client_socket.sendall(b'550 Directory not found\r\n')

def handle_working_dir(client_socket):
    response = f'257 "{DIR}"is the current directory\r\n'
    client_socket.sendall(response.encode())

def handle_create(client_socket, dir_name):
    path = os.path.join(DIR,dir_name)
    try:
        os.mkdir(path)
        client_socket.sendall(b'257 Directory created\r\n')
    except FileExistsError:
        client_socket.sendall(b'550 Directory already exists\r\n')
    except FileNotFoundError:
        client_socket.sendall(b'550 Directory not found\r\n')

def handle_port(client_socket, args):
    print('3')
    ip_parts = args[:-2].split(',')
    print(ip_parts)
    ip_address = '.'.join(ip_parts)
    port = int(args[-2]) * 256 + int(args[-1])
    
    # Establecer conexión de datos con el cliente
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.connect((ip_address, port))
    
    response = '200 PORT command successful\r\n'
    client_socket.sendall(response.encode())

# Función para manejar el comando PASV
def handle_pasv(client_socket):
    print('6')
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.bind(('localhost', 0))  # Elige un puerto aleatorio
    data_socket.listen(1)
    
    ip, port = data_socket.getsockname()
    port_bytes = bytes(','.join(str(port).split()), 'utf-8')
    
    response = f'227 Entering Passive Mode (127,0,0,1,{port_bytes.decode()})\r\n'
    client_socket.sendall(response.encode())
    
    # Aceptar conexión de datos
    data_connection, _ = data_socket.accept()
    
    # Lógica de transferencia de datos aquí
    # Ejemplo: data_connection.sendall(b'Hello from server')
    
    data_connection.close()
    data_socket.close()



def handle_client(client_socket):
    client_socket.sendall(b'220 FTP Server Ready\r\n')
    username=''
    authenticated = False

    while True:
        data = client_socket.recv(1024).decode().strip()
        if not data:
            break

        command, *args = data.split()
        command = command.upper()
        print(command)

        if command == 'USER':
            username=args[0]
            if username in VALID_USERS:
                current_user=username
                client_socket.sendall(b'331 Password required for ' + username.encode() + b'\r\n')
            else:
                client_socket.sendall(b'530 Invalid username\r\n')
        elif command == 'PASS':
            passw=args[0]
            if current_user in VALID_USERS and passw == VALID_USERS[current_user]:
                authenticated = True
                client_socket.sendall(b'230 User logged in\r\n')
            else:
                client_socket.sendall(b'530 Login incorrect\r\n')

        elif command == 'QUIT':
            client_socket.sendall(b'221 Goodbye\r\n')
            break
        elif command == 'LIST':
            if authenticated:
                print('3')
                handle_list(client_socket)
            else:
                client_socket.sendall(b'530 Please login with USER and PASS\r\n')
        elif command == 'RETR':
            if authenticated:
                filename = args[0]
                handle_download(client_socket, filename)
            else:
                client_socket.sendall(b'530 Please login with USER and PASS\r\n')
        elif command == 'STOR':
            if authenticated:
                filename = args[0]
                handle_upload(client_socket, filename)
            else:
                client_socket.sendall(b'530 Please login with USER and PASS\r\n')
        elif command == 'DELE':
            if authenticated:
                filename = args[0]
                handle_del(client_socket, filename)
            else:
                client_socket.sendall(b'530 Please login with USER and PASS\r\n')
        elif command == 'CWD':
            if authenticated:
                directory = args[0]
                handle_change_dir(client_socket, directory)
            else:
                client_socket.sendall(b'530 Please login with USER and PASS\r\n')
        elif command == 'MKD':
            if authenticated:
                directory = args[0]
                handle_create(client_socket, directory)
            else:
                client_socket.sendall(b'530 Please login with USER and PASS\r\n')
        elif command == 'PWD':
            if authenticated:
                print('2')
                handle_working_dir(client_socket)
            else:
                client_socket.sendall(b'530 Please login with USER and PASS\r\n')

        elif command == 'PORT':
            if authenticated:
                print('4')
            handle_port(client_socket, args[0])
        elif command == 'PASV':
            if authenticated:
                print('5')
            handle_pasv(client_socket)

        elif command == 'TYPE':
            if args[0].upper() == 'I':
                print('1')
                response = '200 Type set to I (binary)\r\n'
                client_socket.sendall(response.encode())
            else:
                 client_socket.sendall(b'504 Command not implemented for that parameter\r\n')
        else:
            client_socket.sendall(b'500 Command not supported\r\n')

server_socket=socket.socket(socket.AF_INET,socket.SOCK_STREAM)

server_address=('192.168.66.239',21)
server_socket.bind(server_address)
server_socket.listen(5)
print('El servidor FTP está escuchando en el puerto 21')


while True:
    client_socket, client_address = server_socket.accept()

    print('Conexión desde', client_address)

    client_handler = threading.Thread(target=handle_client, args=(client_socket,))
    client_handler.start()

