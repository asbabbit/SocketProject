import socket
import random
import os
import csv
import math 
import sys
import threading

UDP_PORT_MIN = 49000
UDP_PORT_MAX = 49499

ipv4 = sys.argv[1]
port = 49000
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server.bind((ipv4, int(port)))
        
class Manager:
    def __init__(self): 
        self.peers = {}
        self.leader = None 
        
    #Message string format: "register <peer_name> <ipv4_address> <m_port> <p_port>"
    def register(self, peer_name, ipv4, m, p):
        if peer_name in self.peers:
            return "FAILURE"
        for peer_info in self.peers.values():
            if peer_info["port_m"] == m or peer_info["port_p"] == p:
                return "FAILURE"
        
        #Peers registered
        self.peers[peer_name] = {
            "ipv4_address": ipv4, 
            "port_m": m, 
            "port_p": p,
            "state": "Free"
        }
        return "SUCCESS"
                
    def setup_dht(self, leader_name, n, year):
        if not leader_name in self.peers:   #Peer_name not registered
            return 'FAILURE'
        elif int(n) < 3:                         #n is not at least 3
            return 'FAILURE'
        elif len(self.peers) < n:           #Fewer than n users are registered with manager
            return 'FAILURE'
        elif self.leader is not None:          #A DHT has already been set up
            return 'FAILURE'
            
        #Leader
        self.leader = leader_name
        self.peers[peer_name]["state"] = "Leader"

        dht = [(leader_name, self.peers[leader_name]["ipv4_address"], self.peers[leader_name]["port_p"])]
  
        free_peers = [peer for peer in self.peers.keys() if peer != leader_name and self.peers[peer]["state"] == "Free"]

        if len(free_peers) < int(n) - 1:
            return "FAILURE"
        selected_peers = random.sample(free_peers, int(n) - 1)

        # Updating the state of the peers
        for peer in selected_peers:
            self.peers[peer]["state"] = "InDHT"
            dht.append((peer, self.peers[peer]["ipv4_address"], self.peers[peer]["port_p"]))

        print ("node0: 72 node1: 75 node3: 76")
        return "SUCCESS", dht


    def dht_complete(self, name):
        if name == self.leader:
            return "SUCCESS"
        else:
            return "FAILURE"
                
manager = Manager()

def send(commmand, ipv4, port):
    command = str(command).encode()
    server.sendto(command, (ipv4, int(port)))

def recv():
    print("[STARTING] server is starting ...")
    while True:
        command, ipv4 = server.recvfrom(1024)
        thread = threading.Thread(target=parse_command, args=(command, ipv4))
        thread.start()

def parse_command(command, ipv4):
    msg = command.decode('utf-8')
    print(msg)
    x = msg.split()
    first = x[0]
    print(x[0]) 
    if first == 'register':
        result = manager.register(x[1], x[2], x[3], x[4])
        print(result)
    elif first == 'setup_dht':
        result = manager.setup_dht(x[1], x[2], x[3]) 
        print(result)
    elif first == 'dht_complete':
        result = peer_manager.dht_complete(x[1]) 
        print(result)
    else:
        print('INCORRECT COMMAND')
        

def main():
    
    recv()
 

if __name__ == '__main__':
    main()

import random
import os
import csv
import math 
import sys
import threading

UDP_PORT_MIN = 49000
UDP_PORT_MAX = 49499

ipv4 = sys.argv[1]
port = 49000
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server.bind((ipv4, int(port)))

class Client:
    def __init__(self, name, ipv4, m, p, state='Free'):
        self.name = name
        self.ipv4 = ipv4
        self.m = m
        self.p = p
        self.state = state
        
class Manager:
    def __init__(self): 
        self.port = port
        self.peers = {}
        self.dht = None
        self.leader = None 
    #Message string format: "register <peer_name> <ipv4_address> <m_port> <p_port>"
    def register(self, peer_name, ipv4, m, p):
        i = 0
        if not self.peers:
          self.peers[i] = Client(peer_name, ipv4, m, p)
          i += 1
          return 'SUCCESS'
                
        for peer in self.peers:
            if peer.name == peer_name:
                return 'FAILURE'
            elif peer.ipv4 == ipv4 and (peer.m == m or peer.p == p):
                return 'FAILURE'
            else:
                #Append new peer to registered peers list in Manager object
                self.peers[i] = Client(peer_name, ipv4, m, p)
                i += 1
                return 'SUCCESS'
                
    def setup_dht(self, leader_name, n, year):
        if not leader_name in self.peers:   #Peer_name not registered
            return 'FAILURE'
        elif n < 3:                         #n is not at least 3
            return 'FAILURE'
        elif len(self.peers) < n:           #Fewer than n users are registered with manager
            return 'FAILURE'
        elif self.dht is not None:          #A DHT has already been set up
            return 'FAILURE'
        else:
            #Peer that gave command is now 'Leader' state
            #Leader placed in DHT in 3-tuple format name, ipv4, p-port
            for peer in self.peers:
                if peer.name == leader_name:
                    peer.state = 'Leader'
                    leader = [peer.name, peer.ipv4, peer.p]
                    self.leader = peer
                    self.dht = [leader]
            #Get n-1 registered users who are 'Free' state
            #Randomly select a peer from registered peers if 'Free'
            #Set selected peer to state 'InDHT' and place in DHT
            while i < n-1:
                index = random.randint(0,n-1)
                peer = self.peers[index]
                if peer == 'Free':
                    peer.state = 'InDHT'
                    inDHT = [peer.name, peer.ipv4, peer.p]
                    self.dht.append(inDHT)
                    i += 1 
                
manager = Manager()

def send(commmand, ipv4, port):
    command = str(command).encode()
    server.sendto(command, (ipv4, int(port)))

def recv():
    print("[STARTING] server is starting ...")
    while True:
        command, ipv4 = server.recvfrom(1024)
        thread = threading.Thread(target=parse_command, args=(command, ipv4))
        thread.start()

def parse_command(command, ipv4):
    msg = command.decode('utf-8')
    print(msg)
    x = msg.split()
    first = x[0]
    print(x[0]) 
    if first == 'register':
        result = manager.register(x[1], x[2], x[3], x[4])
        print(result)
        

def main():
    
    recv()
 

if __name__ == '__main__':
    main()
