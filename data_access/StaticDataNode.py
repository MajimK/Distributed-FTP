from utils.utils_functions import getShaRepr, inbetween
from utils.operations import GRANT, REQUEST, RELEASE, OK
from utils.consts import ROOT, COORDINATOR_PORT
from utils.utils_functions import logger
from utils.file_system import FileData
import shutil
import socket
import json
import os

class StaticDataNode:
    def __init__(self, ip) -> None:
        self.ip = ip
        self.id = getShaRepr(ip)
        self.data: dict[dict[FileData]] = {}
        self.replicated_data: dict[dict[FileData]] = {}
        


    def load_data(self):
        replicated_data_file = os.path.normpath(ROOT + '/' + self.ip + '/' + 'replicated_data.json')
        data_file = os.path.normpath(ROOT + '/' + self.ip + '/' + 'data.json')

        data_empty = False
        replicated_empty = False
        
        if os.stat(replicated_data_file).st_size == 0:
            replicated_empty = True
            print(f"REPLICATED DATA FOR {self.ip} IS EMPTY.")
            self.replicated_data = {}
        if os.stat(data_file).st_size == 0:
            data_empty = True
            print(f"DATA FOR {self.ip} IS EMPTY.")
            self.data = {}
           
        try:
            if not data_empty:
                with open(data_file, 'r') as f:
                    self.data = json.load(f)
                    print(f"LOAD_JSON -> DATA: {self.data}")
        except Exception as e:
            print(f"LOAD_JSON -> {e} -> data_file: {data_file}")

        try:
            if not replicated_empty:
                with open(replicated_data_file, 'r') as f:
                    self.replicated_data = json.load(f)
                    print(f'LOAD_JSON -> REPLICATED: {self.replicated_data}')
        except Exception as e:
            print(f"LOAD_JSON -> {e} -> rep_file: {replicated_data_file}")
        pass
    
    def save_data(self, is_replication: bool):
        """Save this instance to a JSON file."""
        
        file_name = 'data.json' if not is_replication else 'replicated_data.json'
        root_dir = os.path.join(ROOT, self.ip)
        file_path = os.path.join(root_dir, file_name)
        data = self.data if not is_replication else self.replicated_data

        print(f"SAVE_DATA -> {self.data} AND IS_REPLICATION: {is_replication} AND DATA: {data}")

        with open(file_path, 'w') as f:
            json.dump(data, f)
    
    
    def send_message(self, message:str, operation:str, coordinator_ip)->bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((coordinator_ip, COORDINATOR_PORT))
            s.sendall(f'{message},{operation}'.encode('utf-8'))
            response = s.recv(1024).decode('utf-8').strip()
            if response == GRANT or response == OK:
                return True
            else: return False

    def migrate_data_to_new_node(self, new_node_ip: str, pred_node_id, succ_node_ip, coordinator_ip):
        if self.send_message(REQUEST, 'new_node',coordinator_ip):
            self.load_data()
            # print("MIGRATE_DATA_TO_NEW_NODE: " + {self.ip})
            new_node = StaticDataNode(new_node_ip)
            new_node.load_data()
            succ_data_node = StaticDataNode(succ_node_ip)
            succ_data_node.load_data()
            print(f"CUANDO CARGA LOS DATOS SUCC_REP_DATA ES: {succ_data_node.replicated_data}")

            # migrate directories
            keys_to_transfer = [k for k in self.data.keys()]
            for key in keys_to_transfer:
                hash_key = getShaRepr(key)
                if inbetween(hash_key, pred_node_id, new_node.id):
                    new_node.data[key] = self.data[key]
                    del self.data[key]

            # migrate replicated directories
            keys_to_transfer = [k for k in self.replicated_data.keys()]
            for key in keys_to_transfer:
                new_node.replicated_data[key] = self.replicated_data[key]
                del self.replicated_data[key]

            # update self replicated data
            for key in new_node.data:
                self.replicated_data[key] = new_node.data[key]

            # update successor replicated data
            succ_data_node.replicated_data = {}
            succ_data_node.save_data(True)
            for key in self.data:
                succ_data_node.replicated_data[key] = self.data[key]

            
            path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
            new_node_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'DATA')
        
            # clean new node folder before copying
            self.clean_folder(new_node_path) 

            # migrate files
            self.copy_folder_with_condition(path, new_node_path, new_node.id, pred_node_id)
            
            # clean new node replicated data folder before copying
            new_node_replicated_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'REPLICATED_DATA')
            self.clean_folder(new_node_replicated_path)
            
            # migrate replicated files
            replicated_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
            self.copy_files(replicated_path, new_node_replicated_path, True)
            
            # update replicated data of successor
            self.copy_files(new_node_path, replicated_path)

            print(f'NEW NODE DATA: {new_node.data}')
            print(f'NEW NODE REPLICATED DATA: {new_node.replicated_data}')
            print(f'SELF DATA: {self.data}')
            print(f'SELF REPLICATED DATA: {self.replicated_data}')
            print(f'SUCC DATA: {succ_data_node.replicated_data}')
            
            succ_data_node.save_data(True)
            new_node.save_data(False)
            new_node.save_data(True)
            self.save_data(False)
            self.save_data(True)
            response = self.send_message(RELEASE, 'new_node', coordinator_ip)
            if response:
                print('new_node -> RELEASE SENT...')
            else:
                print('new_node -> NADA DE RELEASE')
        else:
            print("new_node -> NO SE PUDO REPLICAR!!!")

    def migrate_data_one_node(self, new_node_ip: str, coordinator_ip):
        if self.send_message(REQUEST, 'one_node', coordinator_ip):
            self.load_data()
            logger.debug(f"MIGRATE_DATA_ONE_NODE: {self.data}")
            logger.debug(f"MIGRATE_DATA_ONE_NODE: {self.replicated_data}")

            new_node = StaticDataNode(new_node_ip)
            new_node.load_data()
            logger.debug(f"MIGRATE_DATA_ONE_NODE: NEW_NODE -> {self.data}")
            logger.debug(f"MIGRATE_DATA_ONE_NODE: NEW_NODE_REP ->{self.replicated_data}")

            keys_to_transfer = [k for k in self.data.keys()]
            for key in keys_to_transfer:
                hash_key = getShaRepr(key)
                if inbetween(hash_key, self.id, new_node.id):    # if the key is in the interval (old_id, new_node.id]
                    new_node.data[key] = self.data[key]
                    del self.data[key]

            # share replicated data
            self.replicated_data = {}
            new_node.replicated_data = {}

            # update new node replicated data
            for key in self.data.keys():
                new_node.replicated_data[key] = self.data[key]

            # update self replicated data
            for key in new_node.data:
                self.replicated_data[key] = new_node.data[key]

            # clean new node folders before copying
            new_node_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'DATA')
            new_node_replicated_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'REPLICATED_DATA')
            rep_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')


            self.clean_folder(new_node_path)
            self.clean_folder(new_node_replicated_path)

            # migrate files
            source_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
            self.copy_folder_with_condition_one(source_path,new_node_path, new_node.id, self.id, rep_path, new_node_replicated_path)        
            
            new_node.save_data(False)
            new_node.save_data(True)
            self.save_data(False)
            self.save_data(True)
            if self.send_message(RELEASE, 'one_node', coordinator_ip):
                print('RELEASE SENT...')
            else:
                print('NADA DE RELEASE')
        else:
            print('one_node -> NO SE PUDO REPLICAR')

    
    def migrate_data_cause_fall(self, pred_node_ip, succ_node_ip, coordinator_ip):
        pred_data_node = StaticDataNode(pred_node_ip)
        pred_data_node.load_data()
        succ_data_node = StaticDataNode(succ_node_ip)
        succ_data_node.load_data()
        self.load_data()
        
        # transfer files from replicated to data
        source_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
        dest_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
        self.copy_files(source_path, dest_path, True)

        # transfer directories from replicated to data
        keys_to_transfer = [k for k in self.replicated_data.keys()]
        for key in keys_to_transfer:
            self.data[key] = self.replicated_data[key]
            del self.replicated_data[key]

        # transfer files from pred to self replicated
        pred_source_path = os.path.normpath(ROOT + '/' + pred_node_ip + '/' + 'DATA')
        node_dest_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
        self.copy_files(pred_source_path, node_dest_path)
        
        # transfer directories from pred to self replicated
        for key in pred_data_node.data:
                self.replicated_data[key] = pred_data_node.data[key]

        # transfer files from self to succ replicated 
        node_source_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
        succ_dest_path = os.path.normpath(ROOT + '/' + succ_node_ip + '/' + 'REPLICATED_DATA')
        self.copy_files(node_source_path, succ_dest_path)


        # transfer directories from self to succ replicated
        for key in self.data:
            succ_data_node.replicated_data[key] = self.data[key]

        print(f"SELF.DATA => {self.data}")
        self.save_data(False)
        print(f"SELF.REPLICATED_DATA => {self.replicated_data}")
        self.save_data(True)
        print(f"PRED.REPLICATED_DATA => {pred_data_node.replicated_data}")
        succ_data_node.save_data(True)
                


    

    
    def copy_files(self, source_path, dest_path, remove=False):
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            new_path = os.path.normpath(dest_path + '/' + file)
            shutil.copy2(file_path, new_path)
            if remove:
                os.remove(file_path)
    
    
    def copy_folder_with_condition(self, source_path, dest_path, dest_id, pred_id):
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            file_hash_path = getShaRepr(file_path)

            if inbetween(file_hash_path, pred_id, dest_id): 
                new_path = os.path.normpath(dest_path + '/' + file)
                shutil.copy2(file_path, new_path)
                os.remove(file_path)

    def copy_folder_with_condition_one(self, source_path, dest_path, dest_id, pred_id, rep_source_path, rep_dest_path):
        
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            file_hash_path = getShaRepr(file_path)

            if inbetween(file_hash_path, pred_id, dest_id): 
                new_path = os.path.normpath(dest_path + '/' + file)
                shutil.copy2(file_path, new_path)
                os.remove(file_path)
            else:
                rep_file_path = os.path.normpath(rep_source_path + '/' + file)
                new_path = os.path.normpath(rep_dest_path + '/' + file)
                shutil.copy2(rep_file_path, new_path)
                os.remove(rep_file_path)

    def clean_folder(self, path):
        # clean new node folder before copying
        for file in os.listdir(path):
            p = os.path.join(path, file)
            os.remove(p) 


    
    
    def create_its_folder(self):
        data_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
        os.makedirs(data_path, exist_ok=True)
        replicated_data_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
        os.makedirs(replicated_data_path, exist_ok=True)

        self.save_data(True)
        self.save_data(False)

    