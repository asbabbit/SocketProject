import socket
import sys
import threading
import csv
import ast

manager_ip = sys.argv[1]
manager_port = sys.argv[2]
peer_ip = sys.argv[3]
m_port = sys.argv[4]
p_port = sys.argv[5]

#Create sockets for both types of connections
#Bind those sockets to the specific address
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind((peer_ip, int(p_port)))
server.bind((peer_ip, int(m_port)))

'''
  DHT structure
  {
    (name0, IP0, P0),
    (name1, IP1, P1),
    (name2, IP2, P2),
    .
    .
    .
    (name_n, IP_n, P_n)
  }
'''

class Peer:
    def __init__(self):
        self.DHT = {}
        self.ringSize = None
        self.id = None
        self.nextIP = None
        self.nextP = None
        self.localTable = None

    def store(self, pos ,id, entry):
        print(self.id)
        print(id)
        print(pos)
        if self.id == id:
            self.localTable[id].append({pos: entry})
            print(self.localTable[id])
        else:
            command = f"store {str(pos)} {str(id)} {str(entry)}"
            send_to_client(command, self.nextIP, self.nextP)
            
    #Prime checker
    def prime(self, num):
        if num < 2:
            return False
        for i in range(2, int(num**0.5) + 1):
            if num % i == 0:
                return False
        return True
        
    #Finds and returns the first prime number larger than 2 * n  
    def primeLarger(self, n):
        s = 2 * n + 1
        while True:
            if self.prime(s):
                break
            else:
                s += 1
        return s

    #Creates and returns dictionary from csv file
    '''
    data format
    {
      {1: {"EVENT_ID": "int"...}},
      {2: {"EVENT_ID": "int"...}},
      .
      .
      .
      {n: {"EVENT_ID": "int"...}}}
    }
    '''    
    def localData(self, year, ringSize):
        data = {}
        filePath = f"/afs/asu.edu/users/a/s/b/asbabbit/Data/details-{str(year)}.csv"
        lineNum = 0
        
        with open(filePath, mode='r') as csv_file:
            reader = csv.DictReader(csv_file)
            next(reader)
            for entry in reader:
                data.update({lineNum:entry})
                lineNum += 1
        #print(data[0])
        return data
            
    def createTable(self, year, ringSize):
        data = self.localData(year, ringSize)
        s = self.primeLarger(len(data))
        
        for i in range(len(data)):
            pos = int(data[i]["EVENT_ID"]) % s
            id = pos % ringSize
            self.store(pos, id, data[i])
        #self.ltable_print()
        #send_to_server(f"dht_complete {self.DHT[0][0]}")
        
    #Print size of peer table   
    def ptable_print(self):
        print(f"{self.id}: {len(self.localTable)}")
        
    #Print size of leader table and send command for other peers to print table   
    def ltable_print(self):
        print(f"{self.id}: {len(self.localTable)}")
        for id in range(1, len(self.DHT)):
            command = "ptable_print"
            send_to_client(command, self.DHT[id][1], self.DHT[id][2])
            
    #Message string format: "set_peer <length of DHT> <id in DHT> <nextIP> <nextP> <YYYY> <DHT>"
    def set_peer(self, ringSize, id, nextIP, nextP, year, DHT):
        self.DHT = DHT
        self.ringSize = ringSize
        self.id = id
        self.nextIP = nextIP
        self.nextP = nextP

    #Message string format: "set_leader <DHT> <YYYY>"
    def set_leader(self, year, DHT):
        self.DHT = DHT
        self.ringSize = len(DHT)
        self.id = 0
        self.nextIP = DHT[1][1]
        self.nextP = DHT[1][2]
        for id in range(1, len(DHT)):
            nextId = (id+1) % (len(DHT))
            nextIP = DHT[nextId][1]
            nextP = DHT[nextId][2]
            command = f"set_peer {self.ringSize} {id} {nextIP} {nextP} {year} {DHT}"
            send_to_client(command, DHT[id][1], DHT[id][2])
        self.createTable(year, self.ringSize)

peer = Peer()  

def send_to_server(command):
    command = str(command).encode()
    server.sendto(command, (manager_ip, int(manager_port)))  
    
def send_to_client(command, ipv4, port):
    command = str(command).encode()
    client.sendto(command, (ipv4, int(port)))

def recvServer():
    while True:
        command, ipv4 = server.recvfrom(1024)
        parse_command(command, ipv4)
        
def recvClient():
    while True:
        command, ipv4 = client.recvfrom(1024)
        parse_command(command, ipv4)
                
#Parse commands sent to peer
def parse_command(command, ipv4):
    msg = command.decode('utf-8')
    x = msg.split()
    first = x[0] 
    if first == 'set_leader':
        temp = ""
        for element in x[2:]:                    #Wierd parsing needed to call function
            temp = temp + element + " "
        DHT = ast.literal_eval(temp)  
        peer.set_leader(x[1], DHT)
    elif first == 'set_peer':
        temp = ""
        for element in x[6:]:                    #Wierd parsing needed to call function
            temp = temp + element + " "
        DHT = ast.literal_eval(temp) 
        peer.set_peer(x[1], x[2], x[3], x[4], x[5], DHT)
    elif first == 'store':
        temp = ""
        for element in x[3:]:                    #Wierd parsing needed to call function
            temp = temp + element + " "
        entry = ast.literal_eval(temp) 
        peer.store(x[1], x[2], entry)
    elif first == 'ptable_print':
        peer.peer_table
            
def main():
    print("[STARTING] client is starting...")
  
    thread_recvServer = threading.Thread(target= recvServer)
    thread_recvServer.start()
    
    thread_recvClient = threading.Thread(target= recvClient)
    thread_recvClient.start()
    
    while True:
        command = input("Enter valid command: ").encode()
        server.sendto(command, (manager_ip, int(manager_port))) 




if __name__ == '__main__':
    main()
