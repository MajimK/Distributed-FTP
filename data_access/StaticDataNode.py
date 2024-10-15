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
        x = os.path.exists(replicated_data_file)
        print(replicated_data_file)
        # print(f"load_data: REP_DATA_FILE -> {x}")

        # if not os.path.exists(replicated_data_file) or not os.path.exists(data_file):
        #     self.create_its_folder()


        if os.stat(replicated_data_file).st_size == 0:
            replicated_empty = True
            # print(f"REPLICATED DATA FOR {self.ip} IS EMPTY.")
            self.replicated_data = {}
        if os.stat(data_file).st_size == 0:
            data_empty = True
            # print(f"DATA FOR {self.ip} IS EMPTY.")
            self.data = {}
           
        try:
            if not data_empty:
                with open(data_file, 'r') as f:
                    self.data = json.load(f)
                    # print(f"LOAD_JSON -> DATA: {self.data}")
        except Exception as e:
            print(f"LOAD_JSON -> {e} -> data_file: {data_file}")

        try:
            if not replicated_empty:
                with open(replicated_data_file, 'r') as f:
                    self.replicated_data = json.load(f)
                    # print(f'LOAD_JSON -> REPLICATED: {self.replicated_data}')
        except Exception as e:
            print(f"LOAD_JSON -> {e} -> rep_file: {replicated_data_file}")
        pass
    
    def save_data(self, is_replication: bool):
        """Save this instance to a JSON file."""
        # print(f'!!!!!!Para {self.ip} datos son -> {self.data} \n Replicados son -> {self.replicated_data}')
        file_name = 'data.json' if not is_replication else 'replicated_data.json'
        root_dir = os.path.join(ROOT, self.ip)
        file_path = os.path.join(root_dir, file_name)
        data = self.data if not is_replication else self.replicated_data

        # print(f"SAVE_DATA -> {self.data} AND IS_REPLICATION: {is_replication} AND DATA: {data}")
        # print(f"save_data: JSON CREADO EN {file_path}")
        with open(file_path, 'w') as f:
            json.dump(data, f)

        with open(file_path, 'r') as f:
            x = json.load(f)
        # print(f"save_data: INFO DEL JSON: {x}")
    
    
    def send_message(self, message:str, operation:str, coordinator_ip)->bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((coordinator_ip, COORDINATOR_PORT))
            s.sendall(f'{message},{operation}'.encode('utf-8'))
            response = s.recv(1024).decode('utf-8').strip()
            if response == GRANT or response == OK:
                return True
            else: return False

    def migrate_data_to_new_node(self, new_node_ip: str, pred_node_ip, succ_node_ip, coordinator_ip):
        if self.send_message(REQUEST, 'new_node',coordinator_ip):
            self.load_data()
            # print("MIGRATE_DATA_TO_NEW_NODE: " + {self.ip})
            # print(f'ESTOS SON LOS IP PREDECESOR{pred_node_ip} Y SUCESOR{succ_node_ip}' )
            new_node = StaticDataNode(new_node_ip)
            new_node.load_data()
            succ_data_node = StaticDataNode(succ_node_ip)
            succ_data_node.load_data()
            pred_data_node = StaticDataNode(pred_node_ip)
            pred_data_node.load_data()
            # print(f"CUANDO CARGA LOS DATOS SUCC_REP_DATA ES: {succ_data_node.replicated_data}")
            
            # remove my data from succ replicated data: 1
            # print('ESTOY EN MIGRATE DATA TO NEW NODE')
            keys_to_transfer = [k for k in succ_data_node.replicated_data.keys()]
            for key in keys_to_transfer:
                if key in self.data:
                    del succ_data_node.replicated_data[key]

            # Take their data: 2
            keys_to_transfer = [k for k in self.data.keys()]
            for key in keys_to_transfer:
                hash_key = getShaRepr(key)
                if inbetween(hash_key, pred_data_node.id, new_node.id):
                    new_node.data[key] = self.data[key]
                    del self.data[key]

            # Replicate data from pred: 3
            keys_to_transfer = [k for k in pred_data_node.data.keys()]
            for key in keys_to_transfer:
                new_node.replicated_data[key] = pred_data_node.data[key]

            # Replicate data from me: 4
            keys_to_transfer = [k for k in self.data.keys()]
            for key in keys_to_transfer:
                new_node.replicated_data[key] = self.data[key]
            
            # Delete my duplicate replicated data except succ's data: 5
            keys_to_transfer = [k for k in self.replicated_data.keys()]
            for key in keys_to_transfer:
                if key in succ_data_node.data:
                    continue
                if key in new_node.replicated_data:
                    del self.replicated_data[key]
            
            # Delete pred replicated data if exist in new node replicated data: 6
            keys_to_transfer = [k for k in pred_data_node.replicated_data.keys()]
            for key in keys_to_transfer:
                if key in new_node.replicated_data:
                    del pred_data_node.replicated_data[key]

            # Copy my Data to succ's replicated data: 7
            keys_to_transfer = [k for k in self.data.keys()]
            for key in keys_to_transfer:
                succ_data_node.replicated_data[key] = self.data[key]
                # print(f'SE HACE ESTO Y ESTA ES LA KEY --> {succ_data_node.replicated_data[key]}')

            # Copy new node data to me and pred: 8
            keys_to_transfer = [k for k in new_node.data.keys()]
            for key in keys_to_transfer:
                if key not in self.replicated_data:
                    self.replicated_data[key] = new_node.data[key]
                if key not in pred_data_node.replicated_data:
                    pred_data_node.replicated_data[key]= new_node.data[key]

            # print('SE SUPONE QUE LLEGUE AQUI')
            

            # TRANSFER FILES
            path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
            replicated_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
            new_node_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'DATA')
            new_node_replicated_path = os.path.normpath(ROOT + '/' + new_node.ip + '/' + 'REPLICATED_DATA')
            succ_node_path = os.path.normpath(ROOT + '/' + succ_data_node.ip + '/' + 'DATA')
            succ_node_replicated_path = os.path.normpath(ROOT + '/' + succ_data_node.ip + '/' + 'REPLICATED_DATA')
            pred_node_path = os.path.normpath(ROOT + '/' + pred_data_node.ip + '/' + 'DATA')
            pred_node_replicated_path = os.path.normpath(ROOT + '/' + pred_data_node.ip + '/' + 'REPLICATED_DATA')

            # 1
            self.remove_duplicates(succ_node_replicated_path,path)
            # print('PASE DE 1')

        
            # clean new node folder before copying
            self.clean_folder(new_node_path) 

            # 2
            self.copy_folder_with_condition(path, new_node_path, new_node.id, pred_data_node.id)
            # print('PASE DE 2')

            # clean new node replicated data folder before copying
            self.clean_folder(new_node_replicated_path)

            # 3
            self.copy_files(path, new_node_replicated_path)
            # print('PASE DE 3')

            # 4
            self.copy_files(pred_node_path, new_node_replicated_path)
            # print('PASE DE 4')

            # 5
            self.remove_duplicates(new_node_replicated_path,succ_node_path,replicated_path)
            # print('PASE DE 5')

            # 6
            self.remove_duplicates(pred_node_replicated_path,new_node_replicated_path)
            # print('PASE DE 6')

            # 7
            self.copy_files(path,succ_node_replicated_path)
            # print('PASE DE 7')


            # 8
            self.copy_files(new_node_path,replicated_path)
            self.copy_files(new_node_path,pred_node_replicated_path)
            # print('PASE DE 8')


            # print(f'NEW NODE DATA: {new_node.data}')
            # print(f'NEW NODE REPLICATED DATA: {new_node.replicated_data}')
            # print(f'SELF DATA: {self.data}')
            # print(f'SELF REPLICATED DATA: {self.replicated_data}')
            # print(f'SUCC DATA: {succ_data_node.replicated_data}')

            if pred_node_ip == succ_node_ip:
                keys_to_transfer = [k for k in pred_data_node.replicated_data.keys()]
                for key in keys_to_transfer:
                    succ_data_node.replicated_data[key] = pred_data_node.replicated_data[key]
            
            pred_data_node.save_data(True)
            succ_data_node.save_data(True)
            new_node.save_data(False)
            new_node.save_data(True)
            self.save_data(False)
            self.save_data(True)
            response = self.send_message(RELEASE, 'new_node', coordinator_ip)
            if response:
                logger.debug("🤝 Recurso devuelto")

            else:
                logger.debug("🤝 Recurso NO devuelto")

        else:
            print("❌❌❌ NO SE PUDO REPLICAR!!!")

    def migrate_data_one_node(self, new_node_ip: str, coordinator_ip):
        # print('ENTRE EN MIGRATE DATA ONE NODE')
        if self.send_message(REQUEST, 'one_node', coordinator_ip):
            self.load_data()
            # logger.debug(f"MIGRATE_DATA_ONE_NODE: {self.data}")
            # logger.debug(f"MIGRATE_DATA_ONE_NODE: {self.replicated_data}")

            new_node = StaticDataNode(new_node_ip)
            new_node.load_data()
            # logger.debug(f"MIGRATE_DATA_ONE_NODE: NEW_NODE -> {self.data}")
            # logger.debug(f"MIGRATE_DATA_ONE_NODE: NEW_NODE_REP ->{self.replicated_data}")

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
                logger.debug("🤝 Recurso devuelto")

            else:
                logger.debug("🤝 Recurso NO devuelto")

        else:
            print("❌❌❌ NO SE PUDO REPLICAR!!!")
      
    def migrate_data_cause_fall(self, pred_node_ip, succ_node_ip, coordinator_ip):
        pred_data_node = StaticDataNode(pred_node_ip)
        pred_data_node.load_data()
        succ_data_node = StaticDataNode(succ_node_ip)
        succ_data_node.load_data()
        self.load_data()
        # coger como dato los datos replicados del predecesor (algo asi)
        # transfer directories from replicated data: 1
        keys_to_transfer = [k for k in self.replicated_data.keys()]
        for key in keys_to_transfer:
            if key in succ_data_node.data:
                continue
            self.data[key] = self.replicated_data[key]
            del self.replicated_data[key]

        # delete my data from pred and take the data of my old predpred 
        keys_to_transfer = [k for k in pred_data_node.replicated_data.keys()]
        for key in keys_to_transfer:
            if key in self.data:
                del pred_data_node.replicated_data[key]
            elif inbetween(getShaRepr(key), getShaRepr(pred_node_ip), getShaRepr(self.ip)):
                self.data[key] = keys_to_transfer[key]
                del pred_data_node.replicated_data[key]
            else:
                continue


        keys_to_transfer = [k for k in succ_data_node.replicated_data.keys()]
        for key in keys_to_transfer:
            if key in self.data:
                del succ_data_node.replicated_data[key]
        

        # transfer directories from pred to self replicated
        keys_to_transfer = [k for k in pred_data_node.data.keys()]
        for key in keys_to_transfer:
                self.replicated_data[key] = pred_data_node.data[key]

        # transfer directories from self to succ and pred replicated
        keys_to_transfer = [k for k in self.data.keys()]
        for key in keys_to_transfer:
            succ_data_node.replicated_data[key] = self.data[key]
            pred_data_node.replicated_data[key] = self.data[key]

        # transfer files from replicated to data
        path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
        replicated_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
        succ_node_path = os.path.normpath(ROOT + '/' + succ_data_node.ip + '/' + 'DATA')
        succ_node_replicated_path = os.path.normpath(ROOT + '/' + succ_data_node.ip + '/' + 'REPLICATED_DATA')
        pred_node_path = os.path.normpath(ROOT + '/' + pred_data_node.ip + '/' + 'DATA')
        pred_node_replicated_path = os.path.normpath(ROOT + '/' + pred_data_node.ip + '/' + 'REPLICATED_DATA')

        self.copy_files_not_dupl(replicated_path, path, succ_node_path)

        self.remove_duplicates(pred_node_replicated_path, path)
        self.remove_duplicates(succ_node_replicated_path, path)


        # transfer files from pred to self replicated
        self.copy_files(pred_node_path, replicated_path)

        # transfer files from self to succ replicated 
        self.copy_files(path, succ_node_replicated_path)
        self.copy_files(path, pred_node_replicated_path)



        # print(f"SELF.DATA => {self.data}")
        self.save_data(False)
        # print(f"SELF.REPLICATED_DATA => {self.replicated_data}")
        self.save_data(True)
        # print(f"PRED.REPLICATED_DATA => {pred_data_node.replicated_data}")
        succ_data_node.save_data(True)
        pred_data_node.save_data(True)
                


    

    
    def copy_files(self, source_path, dest_path, remove=False):
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            new_path = os.path.normpath(dest_path + '/' + file)
            shutil.copy2(file_path, new_path)
            if remove:
                os.remove(file_path)

    
    def copy_files_not_dupl(self, source_path, dest_path, verify_path):
        verify_files = os.listdir(verify_path)
        for file in os.listdir(source_path):
            if file in verify_files:
                continue
            file_path = os.path.normpath(source_path + '/' + file)
            new_path = os.path.normpath(dest_path + '/' + file)
            shutil.copy2(file_path, new_path)
            os.remove(file_path)


    def copy_folder_with_condition(self, source_path, dest_path, dest_id, pred_id):
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            file_to_hash = str(file).replace('-','/')
            file_hash_path = getShaRepr(file_to_hash)

            if inbetween(file_hash_path, pred_id, dest_id): 
                new_path = os.path.normpath(dest_path + '/' + file)
                shutil.copy2(file_path, new_path)
                os.remove(file_path)

    def copy_folder_with_condition_one(self, source_path, dest_path, dest_id, pred_id, rep_source_path, rep_dest_path):
        
        for file in os.listdir(source_path):
            file_path = os.path.normpath(source_path + '/' + file)
            file_to_hash = str(file).replace('-','/')
            file_hash_path = getShaRepr(file_to_hash)

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


    
    
    def create_its_folder(self, is_first = False):
        data_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'DATA')
        path_without_DATA = os.path.normpath(ROOT + '/' + self.ip)
        
        os.makedirs(data_path, exist_ok=True)
        # print(f'DATA DE {self.ip}: {data_path}')
        replicated_data_path = os.path.normpath(ROOT + '/' + self.ip + '/' + 'REPLICATED_DATA')
        os.makedirs(replicated_data_path, exist_ok=True)
        # print(f'REP_DATA DE {self.ip}: {replicated_data_path}')
        
        self.clean_folder(data_path)
        self.clean_folder(replicated_data_path)
        
        if is_first:
            self.data[ROOT] = {}
            self.replicated_data[ROOT] = {}

        self.save_data(True)
        self.save_data(False)
        
        x = os.path.exists(path_without_DATA+'/'+'data.json')
        # print(f'DATA.JSON EXISTE: {x}')
        x = os.path.exists(path_without_DATA+'/'+'replicated_data.json')
        # print(f'REP_DATA.JSON EXISTE: {x}')


    def remove_duplicates(self, path_to_del, path_to_verify, path_of_data=None):
        if path_of_data:
            files_to_verify = os.listdir(path_to_verify)
            files_to_delete = os.listdir(path_to_del)
            for file in os.listdir(path_of_data):
                if file in files_to_verify:
                    continue
                if file in files_to_delete:
                    os.remove(file)
        else:
            files_origin = os.listdir(path_to_verify)
            for file in os.listdir(path_to_del):
                if file in files_origin:
                    print(f'FILE IS {file}')
                    os.remove(path_to_del+'/'+file)


    