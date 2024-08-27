import socket, threading
from utils import send_by_broadcast, logger, bully
import time
from consts import BROADCAST_PORT
#--- messages region ---#
ELECTION = 0
COORDINATOR = 1
FEEDBACK = 2
PORT = 8002
# no necesito el id porque cada nodo tiene un elector nodo que le corresponde y todos tienen la propiedad is_coordinator
class BroadcastElectorNode:   #veamos esto como que tiene el id del proceso/nodo o al menos una referencia a el (NO?) al que corresponde
    def __init__(self, id: int) -> None:
        self.id = id   # ahora mismo el id del ElectorNode coincide con el otro id
        self.ip = socket.gethostbyname(socket.gethostname())
        self.coordinator = None   # no estoy seguro que necesite esto
        self.is_coordinator = False
        self.is_in_election = False

    
    def get_coordinator(self):
        return self.coordinator 
    
    def coordinator_loss(self):
        self.coordinator = None
    
    def adopt_coordinator(self, coordinator: str):
        self.coordinator = coordinator
        if coordinator == self.ip:
            self.is_coordinator = True

    def start_election(self):
        t = threading.Thread(target=send_by_broadcast,args=(f'{ELECTION}'))
        t.start()
        # logger.debug(f"start_election: ELECCION COMENZADA POR {self.id}")

    def end_election(self):
        t = threading.Thread(target=send_by_broadcast, args=(f'{COORDINATOR}'))
        t.start()
        # logger.debug("end_election: ELECCION TERMINADA")

    def process_election(self):
        t = threading.Thread(target=self.start_election_server)
        t.start()

        # logger.debug("ENTRA AL PROCESS_ELECTION")
        counter = 0
        while True:
            if not self.coordinator and not self.is_in_election:
                self.is_in_election = True
                self.start_election()
                # logger.debug(f"elect: {self.id} HA LLAMADO A ELECCIONES")

            elif self.is_in_election:
                counter+=1
                if counter == 5:
                    if not self.coordinator or bully(self.ip, self.coordinator):
                        self.coordinator = self.ip
                        self.is_in_election =False
                        self.end_election()
                        # logger.debug(f"elect: {self.id} DA POR CONCLUIDAS LAS ELECCIONES")
                    counter = 0
                    self.is_in_election = False
            else: 
                pass
                # logger.debug(f'elect: EL COORDINADOR ES: {self.coordinator}')
            
            time.sleep(1)


    def start_election_server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        s.bind(('', PORT))    # se vincula a todas las ips disponibles que usan ese puerto

        while True:
            try:
                msg, sender = s.recvfrom(1024)
                ip = sender[0]
                if msg and msg != '':
                    msg = msg.decode('utf-8')
                    operation = int(msg)

                    if operation == ELECTION and not self.is_in_election:
                        # logger.debug(f"start_server_election: ELECTION MESSAGE SENT BY {self.ip}")
                        self.is_in_election = True
                        self.start_election()

                        if bully(self.ip, ip):
                            # it considers itself candidate
                            socket_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            socket_sender.sendto(f'{FEEDBACK}', (ip,PORT))

                    elif operation == FEEDBACK:
                        # logger.debug(f"start_server_election: FEEDBACK MESSAGE SENT BY {self.ip}")
                        if self.coordinator and bully(ip, self.coordinator):
                            self.coordinator = ip
                        self.is_coordinator = False
                        
                    elif operation == COORDINATOR:
                        # logger.debug(f"start_server_election: COORDINATOR MESSAGE SENT BY {self.ip}")
                        if not bully(self.ip, ip) and (not self.coordinator or bully(ip, self.coordinator)):
                            self.coordinator = ip
                            self.is_in_election = False
                            self.is_coordinator = False if self.ip == ip else True 

            except Exception as e:
                pass
                # logger.debug(f"start_election_server: ENTRO A LA EXCEPCION {e}")



        




