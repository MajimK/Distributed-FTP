import socket
import os
import random
import time
# Configuración del servidor FTP
SERVER_IP = 'localhost'  # Dirección IP del servidor FTP
CONTROL_PORT = 2121  # Puerto de control del servidor FTP
DATA_PORT = 8008  # Puerto de datos del servidor FTP (usualmente 20)
BUFFER_SIZE = 4096
def reset_socket(s: socket.socket, target_ip, target_port) -> socket.socket:
    s.close()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((target_ip, target_port))
    s.settimeout(10)
    return s

def send_command(client_socket:socket.socket, command):
    """Envía un comando al servidor y espera una respuesta."""
    try:
        print(command + '\n\n')
        client_socket.send(command.encode('utf-8'))
        response = client_socket.recv(1024).decode('utf-8').strip()
        print(f"Servidor: {response}")
        return response
    except TimeoutError:
        client_socket = reset_socket(client_socket, SERVER_IP, CONTROL_PORT)
        send_command(client_socket, command)

def pasv(command, client_socket):
        port=DATA_PORT
        data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        ip = socket.gethostbyname(socket.gethostname()) 
        print(f'ESTE ES MI IP: {ip}')
        data_socket.bind((ip, port))  
        data_socket.listen(1)
        ip = ip.replace('.', ',')
        p1, p2 = divmod(port, 256)
        response = f'{command},{ip},{p1},{p2}'
        client_socket.sendall(response.encode())
        print('ENVIADO COMANDO')
        response = client_socket.recv(1024).decode().strip()
        print('ESPERANDO RESPUESTA DEL SERVIDOR')
        print(f"Servidor: {response}")
        

        data_transfer_socket, _ = data_socket.accept()
        return data_transfer_socket

def stor_file(file_name, server_ip, control_port, data_port):
    # Establecer conexión con el servidor FTP (puerto de control)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_ip, control_port))
    
    # Esperar mensaje de bienvenida del servidor
    # response = client_socket.recv(1024).decode().strip()
    # print(f"Servidor: {response}")
    client_socket.settimeout(10)

    # Enviar comando para crear los data_transfer
    # command = f'MKD,dir1'
    # response = send_command(client_socket,command)
    # print(f'MKD dir1 Response: {response}')
    # time.sleep(3)

    # command = f'MKD,dir2'
    # response = send_command(client_socket, command)
    # print(f'MKD dir2 Response: {response}')
    # time.sleep(8)

    # command = f'RMD,dir2'
    # response = send_command(client_socket, command)
    # print(f'RMD dir2 Response: {response}')
    # time.sleep(2)

    
    #print('VOY A ENTRAR A PORT')
    command = f'PASV'
    data_socket = send_command(client_socket, command)
    print(data_socket)


    # Enviar comando STOR al servidor
    # file_path = os.path.abspath(file_name)  # Ruta absoluta del archivo a subir
    # print(str(file_path) + '\n')
    # command = f"STOR,{file_path}"
    # response = send_command(client_socket, command)
    
    # Si el servidor está listo para la transferencia
    # if response.startswith("150"):
    #     # Establecer conexión de datos
    #     data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     data_socket.connect((server_ip, data_port))
        
    #     # Abrir el archivo en modo binario y enviar su contenido
    #     with open(file_name, "rb") as file:
    #         while True:
    #             data = file.read(BUFFER_SIZE)
    #             if not data:
    #                 break
    #             data_socket.sendall(data)
        
    #     # Cerrar el socket de datos después de la transferencia
    #     data_socket.close()
        
    #     # Esperar confirmación de transferencia completa del servidor
    #     response = client_socket.recv(1024).decode().strip()
    #     print(f"Servidor: {response}")
    
    # Cerrar el socket de control
    print('Cerrando el socket...')
    client_socket.close()

if __name__ == "__main__":
    file_name = "example.txt"  # Nombre del archivo a subir
    stor_file(file_name, SERVER_IP, CONTROL_PORT, DATA_PORT)