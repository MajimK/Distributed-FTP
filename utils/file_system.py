from utils.utils_functions import *

class FileSystemEntity:
    def __init__(self, name: str, path: str, size: float = 0.0) -> None:
        self.name = name
        self.path = path
        self.size = size

class File(FileSystemEntity):  
    def __init__(self, name, path, size) -> None:
        super().__init__(name, path, size)
        self.directory: Directory = None
        self.extension = None

        
class Directory(FileSystemEntity):
    def __init__(self, name, path, size) -> None:
        super().__init__(name, path, size)
        self.hash_name: str = getShaRepr(name)
        self.files: List[FileSystemEntity] = []