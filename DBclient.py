import socket
from struct import pack,unpack
import time

types = ['string','integer','uinteger','float','long','ulong','double','boolean']

class DB:
    def __init__(self,ip:str,port:int):
        self.ip = ip
        self.port = port
        self.socket = socket.socket()
    def connect(self):
        self.socket.connect((self.ip,self.port))
    
    def create_db(self,name:str,count_of_columns:int,count_of_rows:int):
        packet = b'ndb\0'+bytes(name,'utf-8')+b'\0'+pack('I',count_of_columns)+pack('I',count_of_rows)
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)

    def init_column(self,number_of_db:int,column:int,column_name:str,column_type:str):
        if column_type not in types:
            raise Exception('Unknown type')
        packet = b'ico\0'+pack('I',number_of_db)+pack('I',column)+bytes(column_name,'utf-8')+b'\0'+bytes(column_type,'utf-8')+b'\0'
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)

    def set_value(self,number_of_db:int,column_name:str,cell:int,value_type:str,value):
        if value_type not in types:
            raise Exception('Unknown type')
        packet = b'sva\0'+pack('I',number_of_db)+bytes(column_name,'utf-8')+b'\0'+pack('I',cell)
        if value_type == types[0]:
             packet+=bytes(value,'utf-8')+b'\0'
        elif value_type == types[1]:
             packet+= pack('i',value)
        elif value_type == types[2]:
             packet+= pack('I',value)
        elif value_type == types[3]:
             packet+= pack('f',value)
        elif value_type == types[4]:
             packet+= pack('l',value)
        elif value_type == types[5]:
             packet+= pack('L',value)
        elif value_type == types[6]:
             packet+= pack('d',value)
        elif value_type == types[7]:
             packet+= pack('b',value)
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)

    def get_value(self,number_of_db:int,column_name:str,cell:int,value_type:str):
        if value_type not in types:
            raise Exception('Unknown type')
        packet = b'gva\0'+pack('I',number_of_db)+bytes(column_name,'utf-8')+b'\0'+pack('I',cell)
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(4)
        v_size = unpack('I',dt)[0]
        received = self.socket.recv(v_size)
        
        if value_type == types[0]:
             data=received[:-1].decode('utf-8')
        elif value_type == types[1]:
             data= unpack('i',received)
        elif value_type == types[2]:
             data= unpack('I',received)
        elif value_type == types[3]:
             data= unpack('f',received)
        elif value_type == types[4]:
             data= unpack('l',received)
        elif value_type == types[5]:
             data= unpack('L',received)
        elif value_type == types[6]:
             data= unpack('d',received)
        elif value_type == types[7]:
             data= unpack('b',received)

        return data

    def dump(self):
        packet = b'dum\0'
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)

    def close(self):
        self.socket.close()



if __name__ == '__main__':
   db = DB('127.0.0.1',666)
   db.connect()
   db.create_db('Name',2,1000)
   db.init_column(0,0,'First','integer')
   db.init_column(0,1,'Second','integer')
   nu = 666
   while True:
       tm = time.time()
       db.set_value(0,'First',0,'integer',nu)
       num = db.get_value(0,'First',0,'integer')
       print(time.time()-tm)
       time.sleep(0.5)
       nu += 1
   db.dump()
   db.close()
   print(num)
