# from utils.utils_functions import *
import json
import os

class FileData():
    def __init__(self, permissions_and_type, name:str, size = 0, last_modification_date = None) -> None:
        self.permissions_and_type = permissions_and_type
        self.hard_links = 1
        self.user_id = 0
        self.group_id = 0
        self.size = size
        self.last_modification_date = last_modification_date
        self.name = name

    def to_dict(self):
        return {
            'permissions_and_type': self.permissions_and_type,
            'hard_links': self.hard_links,
            'user_id': self.user_id,
            'group_id': self.group_id,
            'size': self.size,
            'last_modification_date': self.last_modification_date,
            'name': self.name
        }
    
    def from_dict(self, data):
        self.permissions_and_type = data['permissions_and_type']
        self.hard_links = data['hard_links']
        self.user_id = data['user_id']
        self.group_id = data['group_id']
        self.size = data['size']
        self.last_modification_date = data['last_modification_date']
        self.name = data['name']
        return self

    
    def is_dir(self):
        return self.permissions_and_type.startswith('d')
    
    def __repr__(self) -> str:
        return f'{self.permissions_and_type} {self.hard_links} {self.user_id} {self.group_id} {self.size} {self.last_modification_date} {self.name}'

    
    def __str__(self) -> str:
        return f'{self.permissions_and_type} {self.hard_links} {self.user_id} {self.group_id} {self.size} {self.last_modification_date} {self.name}'
    


# def save_data(data, ip, is_replicated: bool):
#         """Save this instance to a JSON file."""
#         file_name = 'data.json' if not is_replicated else 'replicated_data.json'
#         x = os.getcwd()
#         print(x)
#         root_dir = os.path.join(f'{x}/database', ip)
#         file_path = os.path.join(root_dir, file_name)
#         # data = self.replicated_data if is_replicated else data
#         # os.makedirs(file_path, exist_ok=True)
#         print(file_path)
#         with open(file_path, 'w') as f:
#             json.dump(data, f)




# x = {'/app/database/dir1': {},
# '/app/database': 
#     {'/app/database/dir1': 'drwxr-xr-x 1 0 0 0 Sep 08 21:15 dir1', 
#     '/app/database/dir2': 'drwxr-xr-x 1 0 0 0 Sep 08 21:15 dir2'},
# '/app/database/dir2': {}
# }

# save_data(x,'172.17.0.3', False)
