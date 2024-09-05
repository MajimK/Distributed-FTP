import socket
import os

# Configuración del servidor FTP
SERVER_IP = '172.17.0.2'  # Dirección IP del servidor FTP
CONTROL_PORT = 21  # Puerto de control del servidor FTP
DATA_PORT = 8008  # Puerto de datos del servidor FTP (usualmente 20)
BUFFER_SIZE = 4096

def send_command(client_socket, command):
    """Envía un comando al servidor y espera una respuesta."""
    client_socket.sendall(command.encode())
    response = client_socket.recv(1024).decode().strip()
    print(f"Servidor: {response}")
    return response

def stor_file(file_name, server_ip, control_port, data_port):
    # Establecer conexión con el servidor FTP (puerto de control)
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_ip, control_port))
    
    # Esperar mensaje de bienvenida del servidor
    response = client_socket.recv(1024).decode().strip()
    print(f"Servidor: {response}")

    # Enviar comando STOR al servidor
    file_path = os.path.abspath(file_name)  # Ruta absoluta del archivo a subir
    command = f"STOR {file_path}\r\n"
    response = send_command(client_socket, command)
    
    # Si el servidor está listo para la transferencia
    if response.startswith("150"):
        # Establecer conexión de datos
        data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        data_socket.connect((server_ip, data_port))
        
        # Abrir el archivo en modo binario y enviar su contenido
        with open(file_name, "rb") as file:
            while True:
                data = file.read(BUFFER_SIZE)
                if not data:
                    break
                data_socket.sendall(data)
        
        # Cerrar el socket de datos después de la transferencia
        data_socket.close()
        
        # Esperar confirmación de transferencia completa del servidor
        response = client_socket.recv(1024).decode().strip()
        print(f"Servidor: {response}")
    
    # Cerrar el socket de control
    client_socket.close()

if __name__ == "__main__":
    file_name = "example.txt"  # Nombre del archivo a subir
    stor_file(file_name, SERVER_IP, CONTROL_PORT, DATA_PORT)
