import socket
import threading

# Configuración del servidor
HOST = '127.0.0.1'  # Dirección IP local
PORT = 8001         # Puerto de escucha

def handle_client(client_socket, client_address):
    print(f"[INFO] Conexión establecida con {client_address}")
    
    try:
        while True:
            # Recibe datos del cliente
            request = client_socket.recv(1024).decode('utf-8')
            
            if not request:
                break

            print(f"[RECV] Mensaje recibido de {client_address}: {request}")

            # Procesa la solicitud y envía una respuesta
            response = f"Echo: {request}"
            client_socket.send(response.encode('utf-8'))

    except Exception as e:
        print(f"[ERROR] Error al manejar al cliente {client_address}: {e}")
    
    finally:
        # Cierra la conexión con el cliente
        print(f"[INFO] Conexión cerrada con {client_address}")
        client_socket.close()

def start_server():
    # Crea un socket TCP
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Asocia el socket con la dirección IP y el puerto
    server.bind((HOST, PORT))
    
    # Habilita el servidor para aceptar conexiones, con una cola de hasta 5 conexiones
    server.listen(5)
    print(f"[INFO] Servidor escuchando en {HOST}:{PORT}")

    while True:
        # Espera nuevas conexiones
        client_socket, client_address = server.accept()
        
        # Crea un nuevo hilo para manejar al cliente
        client_handler = threading.Thread(target=handle_client, args=(client_socket, client_address))
        client_handler.start()

if __name__ == "__main__":
    try:
        start_server()
    except KeyboardInterrupt:
        print("[INFO] Servidor detenido")
