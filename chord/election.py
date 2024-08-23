import socket, threading
from utils import send_by_broadcast
import time
PORT = 8002
#--- messages region ---#
ELECTION = 0
COORDINATOR = 1
FEEDBACK = 2

# no necesito el id porque cada nodo tiene un elector nodo que le corresponde y todos tienen la propiedad is_coordinator
class BroadcastElectorNode:   #veamos esto como que tiene el id del proceso/nodo o al menos una referencia a el (NO?) al que corresponde
    def __init__(self, id: int) -> None:
        self.id = id   # ahora mismo el id del ElectorNode coincide con el otro id
        self.ip = socket.gethostbyname(socket.gethostname())
        self.coordinator = None   # no estoy seguro que necesite esto
        self.is_coordinator = True
        self.is_in_election = False

    def comparer(ip1 ,ip2 ):
        return int(ip1.split('.')[-1]) > int(ip2.split('.')[-1])
    
    def start_election(self):
        t = threading.Thread(target=send_by_broadcast,args=(f'{ELECTION}', PORT))
        t.start()
        print(f"start_election: ELECCION COMENZADA POR {self.id}")

    def end_election(self):
        t = threading.Thread(target=send_by_broadcast, args=(f'{COORDINATOR}',PORT))
        t.start()
        print("end_election: ELECCION TERMINADA")

    def process_election(self):
        t = threading.Thread(target=self.start_election_server)
        t.start()

        while True:
            if not self.coordinator and not self.is_in_election:
                self.is_in_election = True
                self.start_election()
                print(f"elect: {self.id} HA LLAMADO A ELECCIONES")

            elif self.is_in_election and not self.coordinator:
                if self.is_coordinator:
                    self.coordinator = self.ip
                    self.is_in_election =False
                    self.end_election()
                    print(f"elect: {self.id} DA POR CONCLUIDAS LAS ELECCIONES")
            else: 
                print(f'elect: EL COORDINADOR ES: {self.coordinator}')
            
            time.sleep(1)


    def start_election_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        s.bind('', PORT)    # se vincula a todas las ips disponibles que usan ese puerto

        while True:
            try:
                msg, sender = s.recvfrom(1024)
                ip = sender[0]
                if msg and msg != '':
                    msg = msg.decode('utf-8')
                    operation = int(msg)

                    if operation == ELECTION and not self.is_in_election:
                        print("")
                        self.is_in_election = True
                        self.start_election()

                        if self.id > ip:
                            # it considers itself candidate
                            socket_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            socket_sender.sendto(f'{FEEDBACK}', (ip,PORT))

                    elif operation == FEEDBACK:
                        print("")
                        if self.coordinator and ip > self.coordinator:
                            self.coordinator = ip
                        else:
                            self.is_coordinator = False
                        
                    elif operation == COORDINATOR:
                        print("")
                        if self.ip <= ip and ip >= self.coordinator:
                            self.coordinator = ip
                            self.is_in_election = False
                            self.is_coordinator = False if self.ip == ip else True 

            except:
                print("")



        


        pass


