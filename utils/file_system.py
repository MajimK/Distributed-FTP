from utils.utils_functions import *


class FileData():
    def __init__(self, permissions_and_type, name:str, size = 0, last_modification_date = None) -> None:
        self.permissions_and_type = permissions_and_type
        self.hard_links = 1
        self.user_id = 0
        self.group_id = 0
        self.size = size
        self.last_modification_date = last_modification_date
        self.name = name

    def is_dir(self):
        return self.permissions_and_type.startswith('d')
    
    def __repr__(self) -> str:
        return f'{self.permissions_and_type} {self.hard_links} {self.user_id} {self.group_id} {self.size} {self.last_modification_date} {self.name}'

    
    def __str__(self) -> str:
        return f'{self.permissions_and_type} {self.hard_links} {self.user_id} {self.group_id} {self.size} {self.last_modification_date} {self.name}'