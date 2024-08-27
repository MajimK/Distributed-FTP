## FTP Distribuido
Autor:
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

## Cómo ejecutar la solución?  (Aqui va la definitiva pero prondré la actual)
Para ejecutar el proyecto es necesario docker, teniendo docker, se hace lo siguiente:
    1.  Abrir Docker Desktop para que se inicie el servidor de Docker
    2.  Descargar una imagen de Python (se recomienda alguna version slim)
    3.  Abrir una terminal interactiva de la imagen de Python, utilizando como volumen el directorio del proyecto con el siguiente comando:
 ```
 docker run --rm -it -v <path_>:/app <python_img> /bin/bash
 ```
**path**: rutal del proyecto
**python_img**: imagen de Python descargada y su version (Ejemplo: python:3)
    4.  Abrir el directorio app en la terminal:
           ``` cd app ```
    6.  Agregar un primer nodo:
           ``` python3 main.py```
    8.  Agregar un segundo nodo con el comando:
             ```python3 main.py -s```

A continuación se abordará cada una de las soluciones que se le dieron a cada tema.
## Comunicación:
Para la comunicación se usó una DHT, en este caso elegimos Chord porque era fácil de implementar y está bien documentada, en algunos detalles de implementación que se deben conocer tenemos los siguientes:

#### Clase ChordNode:
La clase ```ChordNode``` es sobre la cual parte toda la base de la comunicación y de la red de Chord en general, esta se encarga de controlar la estabilidad de la red; esta clase se instancia con un ```ip``` (la cual se hashea), por lo que funciona como una computadora distribuida.
Las funciones (que tienen que ver con comunicación) de la clase ```ChordNode```:

```start_server```: Básicamente su funcionalidad es iniciar un socket TCP (?) y escuchar solicitudes para procesarlas en paralelo, ya que crea un hilo que ejecuta ```data_receive```
 ```data_receive```: Se encarga de procesar lo que viene de ```start_server```, que es un socket que representa la conexión que se estableció (entre nodos), además de la dirección y los datos, los datos vienen en un formato **operación, id**, donde la primera representa lo que le está pidiendo un nodo a otro (o bien a él mismo) y la segunda el id del nodo vinculado con esa operación, por ejemplo, si un nodo cualquiera quiere saber el sucesor de otro con **id**=x entonces manda un mensaje así **FIND_SUCCESSOR,x** 
 ```start_broadcast_server```: La tarea de esta función la conexión de nodos sin necesidad de pasarle un ip al cual se tienen que conectar, o sea, es una forma de autodescubrimiento, ya que cuando un nuevo nodo se quiere ir lanza un mensaje por **Broadcast** y el primer nodo (ip) que responda será al que se concete, esto está implementado en ```SelfDiscover``` y el flujo es el siguiente:
    1. Nodo nuevo lanza un mensaje con el formato **DISCOVER,ip,port** por broadcast y se queda esperando alguna respuesta escuchando por un socket con su dirección ip y puerto
    2. El primer nodo que le responda le envía un mensaje **ENTRY_POINT** y el que estaba escuchando lo recibe y entonces se conecta a él con el método ```join``` que se explicará más adelante.

```_inbetween```: Recibe un 3 enteros y devuelve si el primero está entre los otros dos, pero como Chord se trata de un anillo entonces también analiza el caso en que el intervalo esté de la forma **final<inicial** por estar el final al "principio" del anillo.
```find_pred```: Lo que hace es buscar el predecesor el **id** que se le pasa cómo parámetro, esto lo hace tomando el primer nodo que cumpla que el **id** pasado está entre el nodo mencionado y su sucesor.
```find_succ```: Se queda con el sucesor del nodo encontrado por ```find_pred```.
```join```:  Si se le pasa la referencia a un nodo (```ChordNodeReference``` de la que se hablará después) entonces toma ```None``` como predecesor y busca su sucesor con ```find_successor``` (de ```ChordNodeReference```) y además se analiza si este es el segundo nodo en el anillo, para notificarlo al otro con el método ```first_notify```, si de lo contrario es el primer nodo en el anillo se inicializa con el predecesor en ```None``` y el sucesor es el mismo.
```notify```: Básicamente lo que hace es rectificar la red para decirle a otro nodo que ahora el id que se le pasa será su predecesor y que este actualice ese valor si es necesario (es mayor que su predecesor o su predecesor está muerto).
```notify_pred```: Se utiliza en un contexto en el que el nodo tiene que actualizar su sucesor, usualemente es cuando se pierde un predecesor y entonces hay que actualizarlo y además notificarle que ahora el que le notifica es su sucesor.
```first_notify```: En el contexto que se habló anteriormente en ```join```, cuando hay solo dos nodos en la red, y este actualiza sus valores de predecesor y sucesor por el único otro nodo diferente a él que hay.
```stabilize```: Es de las funciones que siempre está ejecutándose en un hilo y se ocupa de mantener la red estable.
```check_predecessor```: Chequea constantemente que el predecesor del nodo esté vivo, si está muerto actualiza los valores correspondientes.

#### Clase ChordNodeReference:

ChordNodeReference es una clase auxiliar, que sirve para facilitar la comunicación entre los nodos de chord. Una instancia de ella se asocia a un ChordNode (su referenciado), que es un nodo de la red de chord. Cada instancia de un CNR almacena la dirección IP de su referenciado, y un conjunto de funciones básicas de comunicación, para que otros nodos interactúen con el referenciado. O sea, si tengo un nodo de chord A, entonces un CNR de A tiene su misma IP, y tiene un conjunto de funciones para que otro nodo cualquiera interactúe con él; si B quiere decirle algo a A, tiene que hacerlos a través de las funciones del CNR de A.
Lo más reseñable es el método privado ```_send_data``` que se encarga de enviar mensajes entre nodos con el formato **operación,id**, por ejemplo, en el método ```find_successor``` de esta clase se envía un mensaje de la forma **FIND_SUCCESSOR,id**, donde FIND_SUCCESSOR en realidad es el número que le corresponde a esa operación.

### Coordinación:
De coordinación hasta ahora hay implementado solamente la elección de líder, para esto se utilizó el método bully, que básicamente consiste en que el que tenga el ip mayor se convierte en líder, esto se podía haber hecho por un método de anillo, donde cada nodo pasa su ip y se comparan y al final queda el valor más alto, pero se decidió por optimización usar un método por **Broadcast** lo que significa que ocurre un flujo de la forma que se describe a continuación:
    1. Hay un hilo corriendo continuamente que contiene un servidor que procesa solicitudes relacionadas con las elecciones.
    2. Con el método ```check_coordinator``` que se está ejecutando todo el tiempo, algún nodo se percata que el coordinador cayó (está muerto) y entonces este nodo llama a elecciones (corren el método ```start_election```), llamar a elecciones implica correr un hilo donde se manda un mensaje por Broadcast con el comando **ELECTION**.
    3. Cuando este mensaje llega a las otras direcciones estas se dan por enteradas de que las elecciones comenzaron (corren el ```start_election``` ) se comparan con la que envió el mensaje y si son mayores entonces mandan un mensaje con el comando **FEEDBACK** dando a entender que se postulan para las elecciones como candidatas fuertes.
    4. Esto termina cuando llega a un nodo lo suficientemente "fuerte" y da por terminadas las elecciones con un mensaje de **COORDINATOR**, aquí cada nodo lo asume como coordinador (si están de acuerdo, o sea, si realmente es el mayor) y el proceso termina.







