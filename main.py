from chord.chord_node_reference import ChordNodeReference

cnr = ChordNodeReference('localhost')

cnr._send_data("PERRO", "HOLA")
# para este codigo recibe el mensaje que esta mas abajo, que es la operacion, id
# cnr.find_successor(3)
# [RECV] Mensaje recibido de ('127.0.0.1', 55671): 1,3