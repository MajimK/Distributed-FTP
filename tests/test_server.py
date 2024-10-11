import hashlib

def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(),16)





print(getShaRepr('perro23999.txt'))