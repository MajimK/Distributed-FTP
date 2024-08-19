import socket
import threading
import sys
import time
from utils import *
from chord.chord_node_reference import ChordNodeReference


FIND_SUCCESSOR = 1
FIND_PREDECESSOR = 2
GET_SUCCESSOR = 3
GET_PREDECESSOR = 4
NOTIFY = 5
INSERT_NODE = 6
REMOVE_NODE = 7
CHECK_PREDECESSOR = 8
CLOSEST_PRECEDING_FINGER = 9
NOTIFY_PRED = 10


class ChordNode:
    def __init__(self, id: int, ip: str, port: int = 8001, m: int = 160):
        self.id = getShaRepr(str(id))
        print(self.id)
        self.ip = ip
        self.port = port
        self.ref = ChordNodeReference(self.ip, self.port)
        self.succ = self.ref  # Initial successor is itself
        self.pred = None  # Initially no predecessor
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next

        # threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        # threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        # threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        # threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread

    def _start_threads(self):
        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread



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
        node = self.find_pred(id)  # Find predecessor of id
        return self.successor if node.id == id else node.successor

    def find_pred(self, id: int) -> 'ChordNodeReference':
        """Finds the predecessor of given node

        Args:
            id (int): Id of given node

        Returns:
            ChordNodeReference: The predecessor
        """
        # posiblemente tenga que analizar casos limites
        node = self
        while not self._inbetween(id, node.id, node.successor.id):
            node = node.pred
        return node

    # ANALIZAR ESTE METODO MEJOR
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

    def join(self, node: 'ChordNodeReference'):
        """Join a Chord network using 'node' as an entry point.

        Args:
            node (ChordNodeReference): The node
        """
        if node:
            self.pred = None
            self.succ = node.find_successor(self.id)
            self.succ.notify(self.ref)
        else:
            self.succ = self.ref
            self.pred = None

    
    def stabilize(self):
        """Regular check for correct Chord structure."""
        
        while True:
            if self.succ.id != self.id:
                print('stabilize')
                if self.succ.check_node() != b'':
                    x = self.succ.pred
                    print(x)
                    if x.id != self.id:
                        if x and self._inbetween(x.id, self.id, self.succ.id):
                            self.succ = x
                        self.succ.notify(self.ref)

            print(f"successor : {self.succ} predecessor {self.pred}")
            time.sleep(10)
        

    def notify(self, node: 'ChordNodeReference'):
        """Informs to a node about another node

        Args:
            node (ChordNodeReference): The node
        """
        print(f'in notify, my id: {self.id} my pred: {node.id}')
        if node.id == self.id:
            pass
        elif not self.pred:
            self.pred = node
            if self.id == self.succ.id:
                self.succ = node
                self.succ.notify(self.ref)
        elif self._inbetween(node.id, self.pred.id, self.id):
            self.pred.notify_pred(node)
            self.pred = node

    def notify_pred(self, node: 'ChordNodeReference'):
        """Exterior call to stabilize network."""
        print(f'in notify pred, my id: {self.id} my succ: {node.id}')
        self.succ = node

    def fix_fingers(self):
        while True:
            to_write = ''
            for i in range(self.m):
                # Calcular el próximo índice de dedo
                next_index = (self.id + 2**i) % 2**self.m
                if self.succ.id == self.id:
                    self.finger[i] = self.ref
                else:
                    if self._inbetween(next_index, self.id, self.succ.id):
                        self.finger[i] = self.succ
                    else:
                        node = self.succ.closest_preceding_finger(next_index)
                        if node.id != next_index:
                            node = node.succ
                        self.finger[i] = node
                
                if i == 0 or self.finger[i-1].id != self.finger[i].id or i > 154:
                    to_write += f'>> ({i}, {next_index}): {self.finger[i].id}\n'
            print(f'fix_fingers {self.id}: {self.succ} and {self.pred}')
            print(f'{self.id}:\n{to_write}')
            time.sleep(10)

    def check_predecessor(self):
        while True:
            try:
                if self.pred and self.pred.check_node() == b'':
                    print("Lost predecessor")
                    self.pred = self.find_pred(self.pred.id)
                    self.pred.notify_pred(self.ref)
            except Exception as e:
                self.pred = None
            time.sleep(10)


    def store_key(self, key: str, value: str):
        """Sotres key-value pair and replicates to the successor

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

    # def data_receive(self, conn: socket, addr, data: list):
    #     data_resp = None
    #     option = int(data[0])
    #     print(f'receive {option} from {data[1]}')
    #     print(f'data: {data}')

    #     if option == FIND_SUCCESSOR:
    #         id = int(data[1])
    #         data_resp = self.find_succ(id)
    #     elif option == FIND_PREDECESSOR:
    #         id = int(data[1])
    #         data_resp = self.find_pred(id)
    #     elif option == GET_SUCCESSOR:
    #         data_resp = self.succ if self.succ else self.ref
    #     elif option == GET_PREDECESSOR:
    #         data_resp = self.pred if self.pred else self.ref
    #     elif option == NOTIFY:
    #         id = int(data[1])
    #         ip = addr[0]
    #         self.notify(ChordNodeReference(id, ip, self.port))
    #     elif option == NOTIFY_PRED:
    #         id = int(data[1])
    #         ip = data[2]
    #         self.notify_pred(ChordNodeReference(id, ip, self.port))
    #     elif option == INSERT_NODE:
    #         id = int(data[1])
    #         ip = data[2]
    #         self.insert_node(ChordNodeReference(id, ip, self.port))
    #     elif option == REMOVE_NODE:
    #         id = int(data[1])
    #         self.remove_node(id)
    #     elif option == CHECK_PREDECESSOR:
    #         pass
    #     elif option == CLOSEST_PRECEDING_FINGER:
    #         id = int(data[1])
    #         data_resp = self.closest_preceding_finger(id)

    #     if data_resp:
    #         print(f'response id: {data_resp.id}, ip: {data_resp.ip}')
    #         response = f'{data_resp.id},{data_resp.ip}'.encode()
    #         conn.sendall(response)
    #     conn.close()

    # def start_server(self):
    #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #         s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #         s.bind((self.ip, self.port))
    #         s.listen(10)

    #         while True:
    #             conn, addr = s.accept()
    #             print(f'new connection from {addr}' )
    #             data = conn.recv(1024).decode().split(',')
                
    #             threading.Thread(target=self.data_receive, args=(conn, addr, data)).start()


if __name__ == "__main__":

    
    print(sys.argv)
    other_node = None
    if len(sys.argv) <= 1:
         raise SystemError("node id is required")
    id = int(sys.argv[1])
    print(id)
    ip = socket.gethostbyname(socket.gethostname())
    print(ip)
    t = ChordNode(id, ip)
    if len(sys.argv) >= 3:
        print('another node')
        other_node = sys.argv[2].split(":")
        o_ip = other_node[1]
        o_id = other_node[0]
        t.join(ChordNodeReference(o_id, o_ip, t.port))
    while True:
        pass

        



    
        