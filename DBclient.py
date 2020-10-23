import socket
from struct import pack,unpack
import time
from threading import Lock

types = ['string','integer','uinteger','float','long','ulong','double','boolean']

Global_DB_Lock = Lock()



class DB:
    def __init__(self,ip:str,port:int):
        self.ip = ip
        self.port = port
        self.socket = socket.socket()
    def connect(self):
        self.socket.connect((self.ip,self.port))
    
    def create_db(self,name:str,count_of_columns:int,count_of_rows:int,optimize_speed=True):
        packet = b'ndb\0'+bytes(name,'utf-8')+b'\0'+pack('I',count_of_columns)+pack('I',count_of_rows)+pack('B',optimize_speed)
        size = pack('I',len(packet))

        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        Global_DB_Lock.release()

    def init_column(self,name_of_db:int,column:int,column_name:str,column_type:str):
        if column_type not in types:
            raise Exception('Unknown type')
        packet = b'ico\0'+bytes(name_of_db,'utf-8')+b'\0'+pack('I',column)+bytes(column_name,'utf-8')+b'\0'+bytes(column_type,'utf-8')+b'\0'
        size = pack('I',len(packet))

        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        Global_DB_Lock.release()

    def set_value(self,name_of_db:int,column_name:str,cell:int,value_type:str,value):
        if value_type not in types:
            raise Exception('Unknown type')
        packet = b'sva\0'+bytes(name_of_db,'utf-8')+b'\0'+bytes(column_name,'utf-8')+b'\0'+pack('I',cell)
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

        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        Global_DB_Lock.release()

    def get_value(self,name_of_db:int,column_name:str,cell:int,value_type:str):
        if value_type not in types:
            raise Exception('Unknown type')
        packet = b'gva\0'+bytes(name_of_db,'utf-8')+b'\0'+bytes(column_name,'utf-8')+b'\0'+pack('I',cell)
        size = pack('I',len(packet))

        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(4)
        v_size = unpack('I',dt)[0]
        received = self.socket.recv(v_size)
        Global_DB_Lock.release()
        
        if value_type == types[0]:
             data=received[:-1].decode('utf-8')
	     return data
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

    def get_row(self,name_of_db:int,row_number:int,typesI:list):
        packet = b'gro\0'+bytes(name_of_db,'utf-8')+b'\0'+pack('I',row_number)
        size = pack('I',len(packet))

        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(8)
        v_size = unpack('Q',dt)[0]
        received = self.socket.recv(v_size)
        Global_DB_Lock.release()

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
        packet = b'whe\0'+bytes(name_of_db,'utf-8')+b'\0'+bytes(column_name,'utf-8')+b'\0'
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
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)

        dt = self.socket.recv(8)
        v_size = unpack('Q',dt)[0]
        received = self.socket.recv(v_size)
        Global_DB_Lock.release()
        if v_size == 0:
            raise Exception('Nothing found')
        
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

    def get_type(self,name_of_db:int,column_name:str):
        packet = b'gty\0'+bytes(name_of_db,'utf-8')+b'\0'+bytes(column_name,'utf-8')
        size = pack('I',len(packet))
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(100)
        Global_DB_Lock.release()
        return dt[:-1].decode('utf-8')
    
    def get_db_name(self,number_of_db:int):
        packet = b'gbn\0'+pack('I',number_of_db)
        size = pack('I',len(packet))
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(100)
        Global_DB_Lock.release()
        return dt[:-1].decode('utf-8')

    def dump(self):
        packet = b'dum\0'
        size = pack('I',len(packet))
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        Global_DB_Lock.release()
    
    def exist(self,name_of_db:int,column_name:str,value_type:str,value):
        if value_type not in types:
            raise Exception('Unknown type')
        packet = b'exi\0'+bytes(name_of_db,'utf-8')+b'\0'+bytes(column_name,'utf-8')+b'\0'
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
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(1)
        Global_DB_Lock.release()
        if dt == b'\x00':
            return False
        else:
            return True
     

    def pop(self,name_of_db:int,row_number:int,typesI:list):
        packet = b'pop\0'+bytes(name_of_db,'utf-8')+b'\0'+pack('I',row_number)
        size = pack('I',len(packet))
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(8)
        v_size = unpack('Q',dt)[0]
        if v_size == 0:
            Global_DB_Lock.release()
            raise Exception('Row uninitialized')
        
        received = self.socket.recv(v_size)
        Global_DB_Lock.release()

        data = []
        offset = 0
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

        
    def append_rows(self,name_of_db:int,count:int):
        packet = b'apr\0'+bytes(name_of_db,'utf-8')+b'\0'+pack('I',count)
        size = pack('I',len(packet))
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        Global_DB_Lock.release()

    def insert(self,name_of_db,row_number,data:list,typesI,append=False):
        if row_number != None:
            packet = b'ins\0'+bytes(name_of_db,'utf-8')+b'\0'+pack('I',row_number)
        else:
            packet = b'app\0'+bytes(name_of_db,'utf-8')+b'\0'
        data_offset = 0
        for type in typesI:
            if type == types[0]:
                 packet+=bytes(data[data_offset],'utf-8')+b'\0'
            elif type == types[1]:
                 packet+= pack('i',data[data_offset])
            elif type == types[2]:
                 packet+= pack('I',data[data_offset])
            elif type == types[3]:
                 packet+= pack('f',data[data_offset])
            elif type == types[4]:
                 packet+= pack('q',data[data_offset])
            elif type == types[5]:
                 packet+= pack('Q',data[data_offset])
            elif type == types[6]:
                 packet+= pack('d',data[data_offset])
            elif type == types[7]:
                 packet+= pack('b',data[data_offset])
            data_offset += 1

        size = pack('I',len(packet))
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        Global_DB_Lock.release()

    def append(self,name_of_db,data,typesI):
        self.insert(name_of_db,None,data,typesI,True)

    def get_count_of_rows(self,name_of_db):
        packet = b'gcr\0'+bytes(name_of_db,'utf-8')+b'\0'
        size = pack('I',len(packet))
        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)
        dt = self.socket.recv(8)
        v_size = unpack('Q',dt)[0]
        received = self.socket.recv(v_size)
        Global_DB_Lock.release()

        return unpack('I',received)[0]

    def where_many(self,name_of_db,column_name,data,type,mode):
        '''
        mode: 0 - ==
			  1 - >
			  2 - <
        '''
        packet = b'whn\0'+bytes(name_of_db,'utf-8')+b'\0'+bytes(column_name,'utf-8')+b'\0'+pack('B',mode)
        if type == types[0]:
            packet+=bytes(data[data_offset],'utf-8')+b'\0'
        elif type == types[1]:
            packet+= pack('i',data)
        elif type == types[2]:
            packet+= pack('I',data)
        elif type == types[3]:
            packet+= pack('f',data)
        elif type == types[4]:
            packet+= pack('q',data)
        elif type == types[5]:
            packet+= pack('Q',data)
        elif type == types[6]:
            packet+= pack('d',data)
        elif type == types[7]:
            packet+= pack('b',data)

        size = pack('I',len(packet))

        Global_DB_Lock.acquire()
        self.socket.send(size)
        self.socket.send(packet)

        dt = self.socket.recv(8)
        v_size = unpack('Q',dt)[0]
        received = self.socket.recv(v_size)
        Global_DB_Lock.release()
        to_return = []

        count = unpack('I',received[0:4])[0]
        
        for i in range(count-1):
            offset = 4*i
            to_return.append(unpack('I',received[4+offset:8+offset])[0])
        return to_return

    def close(self):
        self.socket.close()



   
   
