from ftplib import FTP

def ftp_connect(host, username, password):
    ftp = FTP(host)
    ftp.login(username, password)
    return ftp

def list_files(ftp):
    files = []
    ftp.retrlines('LIST', files.append)
    return files

def download_file(ftp, filename, local_path):
    with open(local_path, 'wb') as local_file:
        ftp.retrbinary(f'RETR {filename}', local_file.write)

def upload_file(ftp, local_path, remote_filename):
    with open(local_path, 'rb') as local_file:
        ftp.storbinary(f'STOR {remote_filename}', local_file)

def ftp_disconnect(ftp):
    ftp.quit()

# Ejemplo de uso:
if __name__ == "__main__":
    # Configura los detalles de conexión
    host = 'localhost'
    username = 'Kevin'
    password = 'Kevin'

    try:
        # Conecta al servidor FTP
        ftp = ftp_connect(host, username, password)
        
        # Lista archivos remotos
        print("Archivos remotos:")
        files = list_files(ftp)
        for file in files:
            print(file)

        # Descarga un archivo
        filename = '21:03 02BackstreetBoys-ShapeofMyHeart.mp3'
        local_path = './descargas/archivo_local.txt'
        download_file(ftp, filename, local_path)
        print(f"Archivo '{filename}' descargado como '{local_path}'")

        # Sube un archivo
        # local_file_to_upload = './uploads/archivo_local.txt'
        # remote_filename = 'archivo_subido.txt'
        # upload_file(ftp, local_file_to_upload, remote_filename)
        # print(f"Archivo '{local_file_to_upload}' subido como '{remote_filename}'")

    finally:
        # Cierra la conexión FTP
        ftp_disconnect(ftp)
