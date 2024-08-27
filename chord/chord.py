import socket
import threading
import time
from utils import *
from chord.chord_node_reference import ChordNodeReference
from chord.election import BroadcastElectorNode
from operations import *
from consts import *


class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        # el parametro election es solo para que tenga en cuenta todo lo de coordinacion.
        self.ip = ip
        self.id = getShaRepr(str(ip))
        self.port = port
        self.ref = ChordNodeReference(self.ip, self.port)
        self.succ: ChordNodeReference = self.ref  # Initial successor is itself
        self.pred: ChordNodeReference = None  # Initially no predecessor
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next
        self.elector: BroadcastElectorNode = BroadcastElectorNode(self.id)
        self._start_threads()


    def _start_threads(self):
        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check pred thread
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
        threading.Thread(target=self._coordinator_checker, daemon=True).start() # Start coordinator checker thread
        threading.Thread(target=self.elector.process_election, daemon=True).start() # Start process election thread
        threading.Thread(target=self.start_broadcast_server, daemon=True).start()

    def _coordinator_checker(self):
        while True:
            time.sleep(10)
            coordinator_node = ChordNodeReference(self.elector.get_coordinator())
            if not coordinator_node.check_node():
                print("_coordinator_checker: COORDINATOR LOST")
                self.elector.coordinator_loss()

    def _inbetween(self, k: int, start: int, end: int) -> bool:
        """Checks if k is in the interval (start, end].

        Args:
            k (int): Value to know if is between start and end
            start (int): Beginning of interval
            end (int): Ending of interval

        Returns:
            bool: True or false if is between or not
        """
        if start < end:
            return start < k <= end
        else:  # The interval wraps around 0
            return start < k or k <= end
        
    def find_succ(self, id: int) -> 'ChordNodeReference':
        """Finds the successor of the given node 

        Args:
            id (int): id of the given node

        Returns:
            ChordNodeReference: The successor
        """
        if id == self.id:
            return self.ref
        node: ChordNodeReference = self.find_pred(id)  # Find predecessor of id
        return self.succ if node.id == id else node.succ

    def find_pred(self, id: int) -> 'ChordNodeReference':
        """Finds the predecessor of given node

        Args:
            id (int): Id of given node

        Returns:
            ChordNodeReference: The predecessor
        """
        node = self
        while not self._inbetween(id, node.id, node.succ.id):
            node = node.succ
        return node.ref if isinstance(node, ChordNode) else node

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Returns the closest predecessor of given node

        Args:
            id (int): Id of given node

        Returns:
            ChordNodeReference: Closest preceding finger
        """
        for i in range(self.m - 1, -1, -1):
            if self.finger[i] and self._inbetween(self.finger[i].id, self.id, id):
                return self.finger[i].closest_preceding_finger(id)
        return self.ref

    def join(self, node: 'ChordNodeReference' = None):
        """Join a Chord network using 'node' as an entry point.

        Args:
            node (ChordNodeReference): The node
        """
        if node:
            logger.debug("join: EL NODO VIENE ESPECIFICADO!")
            self.pred = None
            self.succ = node.find_successor(self.id)
            self.elector.adopt_coordinator(node.get_coordinator())
            print(f"join: EL SUCESOR QUE LE DIO JOIN ES: {self.succ}")
            self.succ.notify(self.ref)
            
            if self.succ.succ.id == self.succ.id:
                self.succ.first_notify(self.ref)
                self.pred = self.succ
        else:
            self.succ = self.ref
            self.pred = None


    
    def stabilize(self):
        """Regular check for correct Chord structure."""
        while True:
            if self.id != self.succ.id:
                
                logger.debug('stabilize: ESTABILIZANDO...')
                logger.debug(f"stabilize: MI SUCESOR ES: {self.succ.id}")
                logger.debug(f"SUCESOR VIVO: {self.succ.check_node()}")
                
                if self.succ.check_node(): #the succ isn't dead
                    
                    logger.debug("stabilize: MI SUCESOR NO ESTA MUERTO!!!")
                    
                    succ_predecessor = self.succ.pred

                    logger.debug(f"stabilize: succ_predecessor ES: {succ_predecessor.id}")

                    if succ_predecessor.id != self.id:   #it's not itself
                        
                        logger.debug("stabilize: NO SOY EL PREDECESOR DE MI SUCESOR\n")

                        if succ_predecessor and self._inbetween(succ_predecessor.id, self.id, self.succ.id):
                            if succ_predecessor.id != self.succ.id:
                                logger.debug("[=X=] stabilize: HAY ALGUN NODO ENTRE MI SUCESOR ACTUAL Y YO.\n")
                            
                                self.succ = succ_predecessor
                        self.succ.notify(self.ref)     
                              
            logger.debug(f"[=X=] succ : {self.succ} pred {self.pred}\n")
            time.sleep(10)


    def notify(self, node: 'ChordNodeReference'):
        """Rectifies the predecessor cause the new node entry

        Args:
            node (ChordNodeReference): The new node
        """
        logger.debug(f'in notify, my id: {self.id} my pred: {node.id}')
        if node.id == self.id:
            pass
        else:
            if self.pred is None:
                self.pred = node
            elif node.check_node():
                if self._inbetween(node.id, self.pred.id, self.id):
                    self.pred  = node
       
        

    def notify_pred(self, node: 'ChordNodeReference'):
        """Exterior call to stabilize network."""

        logger.debug(f'notify_pred: NOTIFICANDO AL PREDECESOR DE ID ES: {self.id}, ID DEL PREDECESOR: {self.pred.id} QUE SU SUCESOR SOY YO MISMO')
        self.succ = node

    def first_notify(self, node: 'ChordNodeReference'):
        """Notice that corresponds to the situation when a node was alone in the network.

        Args:
            node (ChordNodeReference): The new node
        """
        self.succ = node
        self.pred = node

    def fix_fingers(self):
        pass

    def check_predecessor(self):
        while True:
            try:
                if self.pred and not self.pred.check_node():

                    logger.debug("check_predecessor: PREDECESOR PERDIDO\n")
                    self.pred = self.find_pred(self.pred.id)
                    self.pred.notify_pred(self.ref)


            except Exception as e:
                logger.debug(f"[XXX] check_predecessor: ENTRA A LA EXCEPCION: {e}\n")
                self.pred = None
            time.sleep(10)


    def store_key(self, key: str, value: str):
        """Sotres key-value pair and replicates to the succ

        Args:
            key (str): The key
            value (str): The value
        """
        key_hash = getShaRepr(key)
        node = self.find_succ(key_hash)
        node.store_key(key, value)
        self.data[key] = value  
        self.succ.store_key(key, value)


    def retrieve_key(self, key: str) -> str:
        """Retrieve key method to get a value for a given key.

        Args:
            key (str): The key

        Returns:
            str: The respective value
        """
        key_hash = getShaRepr(key)
        node = self.find_succ(key_hash)
        return node.retrieve_key(key)


    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')

                threading.Thread(target=self.data_receive, args=(conn, addr, data)).start()

    
    def start_broadcast_server(self):
        """Method to process incoming requests.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(('',BROADCAST_PORT))
        print(f"start_broadcast_server: COMIENZA A ESCUCHAR")
        
        while True:
            data, addr = s.recvfrom(1024)

            if addr[0] == self.ip:
                print("start_broadcast_server: EL MENSAJE ES DE EL MISMO")
                continue
            else:
                print(f"start_broadcast_server: MENSAJE RECIBIDO DESDE: {addr[0]}")
                data = data.decode().split(',')
                operation = int(data[0])
                
                if operation == DISCOVER:
                    print("start_broadcast_server: DISCOVER OPERATION")
                    sender_ip = data[1]
                    sender_port = int(data[2])
                    
                    print(f'start_broadcast_server: {self.ip} MANDA EL MENSAJE HACIA {sender_ip}:{sender_port}')
                    
                    response = f'{ENTRY_POINT},{self.ip}'

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                        sock.connect((sender_ip, sender_port))
                        sock.sendall(response.encode('utf-8'))
                        print(f"start_broadcast_server: MENSAJE {response} ENVIADO CON EXITO HACIA {sender_ip}:{sender_port}")
                except Exception as e:
                    print(e)


    def data_receive(self, conn: socket, addr, data: list):
        """Decides what it do with the messages

        Args:
            conn (socket): socket to send the responses
            addr (_type_): complete address
            data (list): the content of message
        """
        data_resp = None
        option = int(data[0])
        logger.debug(f'receive {option} from {data[1]}')
        logger.debug(f'data: {data}')

        if option == FIND_SUCCESSOR:
            id = int(data[1])
            data_resp = self.find_succ(id)

        elif option == FIND_PREDECESSOR:
            id = int(data[1])
            data_resp = self.find_pred(id)

        elif option == GET_SUCCESSOR:
            data_resp = self.succ if self.succ else self.ref

        elif option == GET_PREDECESSOR:
            data_resp = self.pred if self.pred else self.ref

        elif option == NOTIFY:
            id = int(data[1])
            ip = addr[0]
            self.notify(ChordNodeReference(ip, self.port))

        elif option == NOTIFY_PRED:
            ip = data[2]
            self.notify_pred(ChordNodeReference(ip, self.port))

        elif option == CHECK_NODE:
            data_resp =  self.ref

        elif option == FIRST_NOTIFY:
            ip = data[2]
            self.first_notify(ChordNodeReference(ip, self.port))
       
        elif option == GET_COORDINATOR:
            coord_ip = self.elector.get_coordinator()
            data_resp = ChordNodeReference(coord_ip)

        # Send response
        if data_resp:
            response = f'{data_resp.id},{data_resp.ip}'.encode()
            conn.sendall(response)
            logger.debug(f"data_recieve: RESPUESTA ENVIADA\n")
        conn.close()





    
        