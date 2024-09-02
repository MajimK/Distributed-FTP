from utils.utils_functions import *

class FileSystemEntity:
    def __init__(self, name: str, path: str, size: float = 0.0) -> None:
        self.name = name
        self.path = path
        self.size = size
        self.files = []
        self.directory = None


class File(FileSystemEntity):  
    def __init__(self, name, path, size, directory) -> None:
        super().__init__(name, path, size)
        self.directory: Directory = directory
        self.extension = None
        
class Directory(FileSystemEntity):
    def __init__(self, name, path, size, files: List[FileSystemEntity] = []) -> None:
        super().__init__(name, path, size)
        self.hash_name: str = getShaRepr(name)
        self.files: List[FileSystemEntity] = files


class FileData():
    def __init__(self, name, path, size = 0, container = None) -> None:
        self.path = path
        self.size = size
        self.container = container
        self.name = name
        pass

    def __repr__(self) -> str:
        return f'{self.name}: {self.path}'
    
    def __str__(self) -> str:
        return f'{self.name}: {self.path}'