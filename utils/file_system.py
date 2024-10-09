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
    