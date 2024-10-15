import socket
import sys

def connect_to_ftp_server(host, port=21):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    return sock

def send_command(sock, command):
    sock.sendall(command.encode('utf-8') + b'\r\n')
    response = sock.recv(1024).decode('utf-8')
    print(f'{command}: {response}')
    return response

def login(sock, username, password):
    send_command(sock, f'USER {username}')
    send_command(sock, f'PASS {password}')

def enter_passive_mode(sock):
    response = send_command(sock, 'PASV')
    start = response.find('(')
    end = response.find(')', start + 1)
    print(f'START: {start}')
    print(f'END: {end}')
    if start == -1 or end == -1:
        raise ValueError("Invalid PASV response")
    numbers = response[start+1:end].split(',')
    ip = '.'.join(numbers[:4])
    port = (int(numbers[4]) << 8) + int(numbers[5])
    return ip, port

def list_files(sock):
    ip, port = enter_passive_mode(sock)
    data_sock = connect_to_ftp_server(ip, port)
    send_command(sock, 'LIST')
    data = data_sock.recv(4096).decode('utf-8')
    data_sock.close()
    print(data)

def download_file(sock, filename):
    ip, port = enter_passive_mode(sock)
    data_sock = connect_to_ftp_server(ip, port)
    send_command(sock, f'RETR {filename}')
    with open(filename, 'wb') as f:
        while True:
            data = data_sock.recv(1024)
            if not data:
                break
            f.write(data)
    data_sock.close()

def upload_file(sock, filename):
    ip, port = enter_passive_mode(sock)
    data_sock = connect_to_ftp_server(ip, port)
    send_command(sock, f'STOR {filename}')
    with open(filename, 'rb') as f:
        while True:
            data = f.read(1024)
            if not data:
                break
            data_sock.sendall(data)
    data_sock.close()

def close_connection(sock):
    send_command(sock, 'QUIT')
    sock.close()

if __name__ == "__main__":
    host = 'localhost'  # Reemplaza con la dirección de tu servidor FTP
    # username = 'Kevin'  # Reemplaza con tu nombre de usuario
    # password = 'Kevin'  # Reemplaza con tu contraseña

    ftp_sock = connect_to_ftp_server(host)
    # login(ftp_sock, username, password)
    
    # print("Listando archivos en el directorio actual:")
    # list_files(ftp_sock)

    # filename_to_download = input('Archivo a descargar')  # Reemplaza con el nombre del archivo que quieres descargar
    # print(f"Descargando archivo {filename_to_download}")
    # download_file(ftp_sock, filename_to_download)

    filename_to_upload = 'upload_example.txt'  # Reemplaza con el nombre del archivo que quieres subir
    print(f"Subiendo archivo {filename_to_upload}")
    upload_file(ftp_sock, filename_to_upload)

    close_connection(ftp_sock)
