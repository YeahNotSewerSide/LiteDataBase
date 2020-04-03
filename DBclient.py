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
             packet+= pack('q',value)
        elif value_type == types[5]:
             packet+= pack('Q',value)
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
             data= unpack('q',received)
        elif value_type == types[5]:
             data= unpack('Q',received)
        elif value_type == types[6]:
             data= unpack('d',received)
        elif value_type == types[7]:
             data= unpack('b',received)

        return data[0]

    def get_row(self,number_of_db:int,row_number:int,typesI:list):
        packet = b'gro\0'+pack('I',number_of_db)+pack('I',row_number)
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(8)
        v_size = unpack('Q',dt)[0]
        received = self.socket.recv(v_size)
        data = []
        offset = 0
        for type in typesI:
            if type == types[0]:
                for i in range(offset,v_size):
                    if received[i] == 0:
                        data.append(received[offset:i].decode('utf-8'))
                        offset = offset + i+1
                        break
                 #data.append(received[:-1].decode('utf-8'))
            elif type == types[1]:
                 data.append(unpack('i',received[offset:offset+4])[0])
                 offset+=4
            elif type == types[2]:
                 data.append(unpack('I',received[offset:offset+4])[0])
                 offset+=4
            elif type == types[3]:
                 data.append(unpack('f',received[offset:offset+4])[0])
                 offset+=4
            elif type == types[4]:
                 data.append(unpack('q',received[offset:offset+8])[0])
                 offset+=8
            elif type == types[5]:
                 data.append(unpack('Q',received[offset:offset+8])[0])
                 offset+=8
            elif type == types[6]:
                 data.append(unpack('d',received[offset:offset+8])[0])
                 offset+=8
            elif type == types[7]:
                 data.append(unpack('b',received[offset:offset+1])[0])
                 offset+=1
        return data

    def where(self,name_of_db:str,column_name,value,value_type,typesI:list):# list [row_number,data...]
        packet = b'whe\0'+bytes(name_of_db,'ascii')+b'\0'+bytes(column_name,'utf-8')+b'\0'
        if value_type == types[0]:
             packet+=bytes(value,'utf-8')+b'\0'
        elif value_type == types[1]:
             packet+= pack('i',value)
        elif value_type == types[2]:
             packet+= pack('I',value)
        elif value_type == types[3]:
             packet+= pack('f',value)
        elif value_type == types[4]:
             packet+= pack('q',value)
        elif value_type == types[5]:
             packet+= pack('Q',value)
        elif value_type == types[6]:
             packet+= pack('d',value)
        elif value_type == types[7]:
             packet+= pack('b',value)

        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)

        dt = self.socket.recv(8)
        v_size = unpack('Q',dt)[0]
        received = self.socket.recv(v_size)
        data = [unpack('I',received[0:4])[0]]
        offset = 4
        for type in typesI:
            if type == types[0]:                
                for i in range(offset,v_size):
                    if received[i] == 0:
                        data.append(received[offset:i].decode('utf-8'))
                        offset = i+1
                        break
                 #data.append(received[:-1].decode('utf-8'))
            elif type == types[1]:
                 data.append(unpack('i',received[offset:offset+4])[0])
                 offset+=4
            elif type == types[2]:
                 data.append(unpack('I',received[offset:offset+4])[0])
                 offset+=4
            elif type == types[3]:
                 data.append(unpack('f',received[offset:offset+4])[0])
                 offset+=4
            elif type == types[4]:
                 data.append(unpack('q',received[offset:offset+8])[0])
                 offset+=8
            elif type == types[5]:
                 data.append(unpack('Q',received[offset:offset+8])[0])
                 offset+=8
            elif type == types[6]:
                 data.append(unpack('d',received[offset:offset+8])[0])
                 offset+=8
            elif type == types[7]:
                 data.append(unpack('b',received[offset:offset+1])[0])
                 offset+=1
        return data

    def get_type(self,number_of_db:int,column_name:str):
        packet = b'gty\0'+pack('I',number_of_db)+bytes(column_name,'utf-8')
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(100)
        return dt[:-1].decode('utf-8')
    
    def get_db_name(self,number_of_db:int):
        packet = b'gbn\0'+pack('I',number_of_db)
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(100)
        return dt[:-1].decode('utf-8')

    def dump(self):
        packet = b'dum\0'
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)
    
    def exist(self,number_of_db:int,column_name:str,value_type:str,value):
        if value_type not in types:
            raise Exception('Unknown type')
        packet = b'exi\0'+pack('I',number_of_db)+bytes(column_name,'utf-8')+b'\0'
        if value_type == types[0]:
             packet+=bytes(value,'utf-8')+b'\0'
        elif value_type == types[1]:
             packet+= pack('i',value)
        elif value_type == types[2]:
             packet+= pack('I',value)
        elif value_type == types[3]:
             packet+= pack('f',value)
        elif value_type == types[4]:
             packet+= pack('q',value)
        elif value_type == types[5]:
             packet+= pack('Q',value)
        elif value_type == types[6]:
             packet+= pack('d',value)
        elif value_type == types[7]:
             packet+= pack('b',value)
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(1)
        if dt == b'\x00':
            return False
        else:
            return True
       
    def append_rows(self,number_of_db:int,count:int):
        packet = b'apr\0'+pack('I',number_of_db)+pack('I',count)
        size = pack('I',len(packet))
        self.socket.send(size)
        self.socket.send(packet)

    def close(self):
        self.socket.close()



if __name__ == '__main__':
   db = DB('127.0.0.1',666)
   db.connect()
   #db.create_db('Videos',2,1000000)
   ###db.create_db('Name',3,1000000)
   #db.init_column(0,0,'id','string')
   #db.init_column(0,1,'views','integer')
   data = db.get_row(0,0,['string','float','float','float'])
   #data = db.get_value(0,'act1',0,'float')
   #data = db.where('Videos','views',0,'integer',['string','integer'])
   #db.init_column(0,2,'Third','long')
   #nu = 666
   #start = 0
   #for i in range(start,1000000):
       
   #db.set_value(0,'env',0,'string','a'*1000)
   #db.set_value(0,'act1',0,'float',0.0)
   #db.set_value(0,'act2',0,'float',0.0)
   #db.set_value(0,'act3',0,'float',0.0)

   #    db.set_value(0,'Second',i,'integer',i)
   #    db.set_value(0,'Third',i,'long',i)

   #db.set_value(0,'Third',999,'long',666224)
   #value = db.get_row(0,0,['string','integer'])#db.where(0,'views\0',0,'integer',['string','integer'])
   #print(value)
   #value = db.where(0,'Second\0',999999,'integer',['string','integer','long'])
   #value = db.exist(0,'Second','integer',1)
   #print(value)  
   #db.dump()
   db.close()
   #print(num)
