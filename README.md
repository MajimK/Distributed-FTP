## FTP Distribuido
Autores:

Jan Carlos Pérez González (C412)

Kevin Majim Ortega Álvarez (C412)

## Problemática:
En el mundo actual de la información digital, la gestión eficiente de archivos y la transferencia de datos son cruciales para el éxito de cualquier organización. Los protocolos de transferencia de archivos, como el FTP (File Transfer Protocol), han sido ampliamente utilizados para este propósito. Sin embargo, las necesidades de las empresas modernas han superado las capacidades de los servidores FTP tradicionales, especialmente en escenarios que requieren un alto rendimiento, escalabilidad y resistencia. 

Aquí es donde entra en juego el FTP distribuido. Este enfoque revolucionario transforma la arquitectura tradicional del FTP, distribuyendo la carga de trabajo a través de múltiples servidores interconectados. Esto permite que los sistemas FTP sean más robustos, flexibles y capaces de manejar grandes volúmenes de datos sin problemas. 

## Objetivo:
El objetivo de este proyecto, aunque está alejado de una solución al problema real, propone un acercamiento lo suficientemente realista, como para percatarse qué resolvería usar el protocolo FTP Distribuido (DFTP a partir de ahora).
* **Mejorar el rendimiento de la transferencia de archivos:** El objetivo principal podría ser aumentar la velocidad y eficiencia de la transferencia de archivos, especialmente cuando se manejan grandes volúmenes de datos. 
* **Aumentar la escalabilidad:**  Un FTP distribuido puede manejar un mayor número de usuarios y conexiones simultáneas, lo que lo hace ideal para organizaciones en crecimiento. 
* **Mejorar la disponibilidad:**  Con varios servidores trabajando juntos, un FTP distribuido es más resistente a errores y fallos, asegurando que los archivos estén siempre disponibles.
* **Reducir los costos:**  Al distribuir la carga de trabajo, se puede optimizar el uso de recursos, lo que puede llevar a una reducción en los costos de hardware y energía.

## Arquitectura del Sistema
El sistema se compone de un nodo central que representa el servidor FTP y varios nodos adicionales donde se almacenan los datos. Estos nodos están organizados en un anillo utilizando el protocolo Chord, lo que permite una eficiente localización y acceso a los archivos distribuidos. Cada nodo en el anillo tiene la capacidad de almacenar datos y redirigir solicitudes a otros nodos en caso de ser necesario.

## Replicación de Datos y Tolerancia a Fallos
El sistema asegura la alta disponibilidad de los archivos mediante la replicación de datos. Cada archivo insertado se replica en el nodo predecesor y sucesor del nodo de inserción, lo que garantiza que si un nodo falla, las copias de los archivos estén disponibles en los otros nodos. De esta manera, el sistema puede soportar la caída de hasta dos nodos consecutivos sin perder información ni interrumpir el servicio.

## Comunicación entre Nodos
La comunicación entre el nodo FTP y los nodos de datos se realiza a través de un puerto específico. Este puerto permite la transmisión de comandos y datos entre los nodos, asegurando que las solicitudes de los clientes puedan ser gestionadas de manera eficiente. A continuación, se presentan los comandos FTP disponibles en el sistema:

* **CWD:** Cambia el directorio de trabajo actual.
* **DELE** (Delete): Elimina un archivo.
* **FEAT** (Features): Lista las características soportadas por el servidor.
* **LIST**: Muestra una lista de los archivos en el directorio actual.
* **MKD** (Make Directory): Crea un nuevo directorio.
* **PASV** (Passive Mode): Habilita el modo pasivo para la transferencia de datos.
* **PORT**: Define un puerto para la transferencia activa de datos.
* **PWD** (Print Working Directory): Muestra el directorio de trabajo actual.
* **QUIT**: Finaliza la sesión FTP.
* **RETR** (Retrieve): Recupera un archivo desde el servidor.
* **RMD** (Remove Directory): Elimina un directorio.
* **SYST**: Devuelve información del sistema.
* **STOR** (Store): Sube un archivo al servidor.
* **TYPE_A / TYPE_I**: Cambia el modo de transferencia de ASCII a binario o viceversa.
* **USER**: Permite el acceso de un usuario al servidor.

## Escalabilidad y Gestión de Nodos
El sistema permite la adición y sustracción dinámica de nodos sin afectar su funcionamiento. Cuando se agrega o se elimina un nodo, el sistema ajusta automáticamente la distribución de los datos para mantener el balance de carga y la replicación. Además, el sistema es capaz de soportar la caída simultánea de hasta dos nodos sin que esto afecte el acceso a los archivos o la integridad de los datos.

## Evaluación del Sistema
El rendimiento del sistema se evaluará utilizando herramientas estándar como FileZilla para asegurar su compatibilidad con los protocolos FTP tradicionales. Las pruebas incluirán la transferencia de archivos, la replicación de datos y la recuperación de nodos caídos, verificando que el sistema distribuido cumple con las expectativas de robustez y escalabilidad en un entorno de producción.

## Despliegue y Ejecución

El sistema está diseñado para ser desplegado y ejecutado utilizando Docker, lo que facilita su portabilidad y escalabilidad. Docker se utiliza para crear contenedores que contienen tanto el nodo FTP como los nodos de datos del anillo de Chord. Esto asegura que las dependencias del sistema estén correctamente gestionadas y que sea sencillo de ejecutar en diferentes entornos.

Para ejecutar el proyecto, es necesario contar con Docker. Siguiendo estos pasos, podrás iniciar el entorno de ejecución:

* Iniciar Docker
* Descargar una imagen de Python: Se recomienda utilizar una imagen de Python con una versión slim. Esto se puede hacer con el siguiente comando:
  
  ``` docker pull python:3-slim ```
  
* Abrir una terminal interactiva de la imagen de Python utilizando el siguiente comando para ejecutar un contenedor de Python, montando el directorio del proyecto como volumen:
  
  ``` docker run --rm -it -v <path_del_proyecto>:/app python:3-slim /bin/bash ```
  
  ```<path_del_proyecto>```: Ruta del directorio donde se encuentra el código del proyecto.
  
* Navegar al directorio de la aplicación: Una vez dentro del contenedor, se accede al directorio de la aplicación con:

  ```cd /app```
  
* Agregar un primer nodo: Para iniciar el primer nodo de datos de Chord, se ejecuta el siguiente comando:

  ```python3 init_first_node.py```
  
* Agregar un segundo nodo: Para iniciar un segundo nodo, se ejecuta:

  ```python3 init_secondary_node.py```
  
* Agregar el nodo que actua como servidor FTP: Para iniciar el servidor, se ejecuta:

  ```python3 init_server.py```
