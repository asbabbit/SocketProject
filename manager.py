import socket
import random 
import sys
import threading

ipv4 = sys.argv[1]
port = 49000
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server.bind((ipv4, int(port)))
        
class Manager:
    def __init__(self): 
        self.peers = {}
        self.DHT = {}
        self.setUp = False 
        
    #Message string format: "register <peer_name> <ipv4> <port_m> <port_p>"
    def register(self, peer_name, ipv4, m, p):
        if peer_name in self.peers:
            return "FAILURE: peer name exists in register"
        for peer_info in self.peers.values():
            if (peer_info["port_m"] == m or peer_info["port_p"] == p) and peer_info["ipv4"] == ipv4:
                return "FAILURE: ports already used"
        
        #Peers registered
        self.peers[peer_name] = {
            "ipv4": ipv4, 
            "port_m": m, 
            "port_p": p,
            "state": "Free"
        }
        return "SUCCESS"
        
    #Message string format: "setup_dht <peer_name> <n> <YYYY>"    
    def setup_dht(self, leader_name, n, year):
        if not leader_name in self.peers:   #Peer_name not registered
            return 'FAILURE: peer not registered'
        elif int(n) < 3:                         #n is not at least 3
            return 'FAILURE: n needs to be greater than or equal to 3'
        elif len(self.peers) < int(n):           #Fewer than n users are registered with manager
            return 'FAILURE: fewer than n registered'
        elif self.DHT is self.setUp:          #A DHT has already been set up
            return 'FAILURE: DHT already exists'
            
        #Leader
        self.peers[leader_name]["state"] = "Leader"
        
        #List setup
        self.DHT = [(leader_name, self.peers[leader_name]["ipv4"], self.peers[leader_name]["port_p"])]
        i = 1
        while i < int(n):
            peer = random.choice(list(self.peers.keys()))
            if self.peers[peer]["state"] == "Free" or peer in self.DHT:
                self.peers[peer]["state"] = "InDHT"
                self.DHT.append((peer, self.peers[peer]["ipv4"], self.peers[peer]["port_p"]))
                i += 1
        print(f"DHT:")
        for entry in self.DHT:       
            print(f"{entry}")      
        #Command from manager to leader       
        command = f"set_leader {year} {self.DHT}"
        send(command, self.peers[leader_name]["ipv4"], self.peers[leader_name]["port_m"])
        
    def dht_complete(self, name):
        if self.peers[name]["state"] == "Leader":
            self.setUp = True
            return "SUCCESS"
        else:
            return "FAILURE: DHT not complete"
    
    def deregister(self, peer_name):
        if peer_name in self.DHT:
            return "FAILURE"
        elif not peer_name in self.peers:
            return "FAILURE"
        else:
            print(self.peers)
            del self.peers[peer_name]
            print(self.peers)
            return "SUCCESS"
    

        



                
manager = Manager()

def send(command, ipv4, port):
    command = str(command).encode()
    server.sendto(command, (ipv4, int(port)))

def recv():
    while True:
        command, ipv4 = server.recvfrom(1024)
        parse_command(command, ipv4)

def parse_command(command, ipv4):
    #try:
        msg = command.decode('utf-8')
        x = msg.split()
        print(msg)
        first = x[0] 
        if first == 'register':
            result = manager.register(x[1], x[2], x[3], x[4])
            print(result)
        elif first == 'setup_dht':
            manager.setup_dht(x[1], x[2], x[3]) 
        elif first == 'dht_complete':
            result = manager.dht_complete(x[1])
            print(result)
        elif first == 'deregister':
            result = manager.deregister(x[1])  
        else:
            print('INCORRECT COMMAND')
    #except:
       # print('INCORRECT COMMAND')

def main():
    print("[STARTING] server is starting...")
    
    recv_thread = threading.Thread(target= recv)
    recv_thread.start()
 

if __name__ == '__main__':
    main()
        
