import socket
from utils import getShaRepr
import logging
from operations import *

# logger configuration
#### here ####

# no necesito el id en la referencia del nodo, es identificable perfectamente por el puerto y el ip
# esto seria lo que es el nodo como tal de Chord, o sea, lo que se encierra en el cuadrado en los esquemas que he hecho
class ChordNodeReference:
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port


    def _send_data(self, op: int, data: str = None):
        """Internal function to send data to referenced node (self)

        Args:
            op (int): Selected operation
            data (str): Data to send

        Returns:
            bytes: Answer code 
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                print(f'sending {op}')
                s.connect((self.ip, self.port))
                s.sendall(f'{op},{data}'.encode('utf-8'))
                if op == CHECK_NODE:
                    response = s.recv(1024)
                    print(f"SEND_DATA: RESPONSE EN CHECK_NODE ES: {response}")
                    return response
                else: return s.recv(1024)
        except Exception as e:
            print(f"Error sending data: {e}")
            return b''
        
    
    def find_successor(self, id: int) -> 'ChordNodeReference':
        """Gets Chord node reference of the given node successor 

        Args:
            id (int): Id of given node

        Returns:
            ChordNodeReference: Successor reference
        """
        response = self._send_data(FIND_SUCCESSOR, str(id)).decode().split(',')
        ip = response[1]
        return ChordNodeReference(ip, self.port)

    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        """Gets Chord node reference of the given node predecessor 

        Args:
            id (int): Id of given node

        Returns:
            ChordNodeReference: Predecessor reference
        """
        response = self._send_data(FIND_PREDECESSOR, str(id)).decode().split(',')
        ip =response[1]
        return ChordNodeReference(ip, self.port)

    @property
    def succ(self) -> 'ChordNodeReference':
        response = self._send_data(GET_SUCCESSOR).decode().split(',')
        ip =response[1]
        return ChordNodeReference(ip, self.port)

    @property
    def pred(self) -> 'ChordNodeReference':
        response = self._send_data(GET_PREDECESSOR).decode().split(',')
        print(f'!!!!!!RESPONSE: {response}')
        ip =response[1]
        return ChordNodeReference(ip, self.port)

    def get_coordinator(self) -> str:
        coordinator = self._send_data(GET_COORDINATOR).decode().split(',')[1]
        return coordinator
    
    def notify(self, node: 'ChordNodeReference'):
        """Notifies to current node about another node 

        Args:
            node (ChordNodeReference): Joined node in the ring 
        """
        self._send_data(NOTIFY, f'{node.id},{node.ip}')

    def notify_pred(self, node: 'ChordNodeReference'):
        """BUSCAR

        Args:
            node (ChordNodeReference): _description_
        """
        self._send_data(NOTIFY_PRED, f'{node.id},{node.ip}')

    def first_notify(self, node: 'ChordNodeReference'):
        self._send_data(FIRST_NOTIFY, f'{node.id},{node.ip}')

    def check_node(self):
        """Checks if the predecessor is alive
        """
        response = self._send_data(CHECK_NODE)
        if response != b'' and len(response.decode()) > 0:
            return True
        return False

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        """Returns the closest node in the finger table of the given node

        Args:
            id (int): Id of given node

        Returns:
            ChordNodeReference: The closest preceding node in finger table
        """
        response = self._send_data(CLOSEST_PRECEDING_FINGER, str(id)).decode().split(',')
        ip =response[1]
        return ChordNodeReference(ip, self.port)
    
    ### FALTA RETRIEVE KEY & STORE KEY

    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)
    

