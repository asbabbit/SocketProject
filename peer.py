import ast
import socket
import sys
import threading

manager_ip = sys.argv[1]
manager_port = sys.argv[2]
peer_ip = sys.argv[3]
m_port = sys.argv[4]
p_port = sys.argv[5]

client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.bind((peer_ip, int(p_port)))
server.bind((peer_ip, int(m_port)))

def send_to_server(command, ipv4, port):
    command = str(command).encode('utf-8')
    client.sendto(command, (ipv4, int(port)))

def send_to_client(command, ipv4, port):
    command = str(command).encode('utf-8')
    client.sendto(command, (ipv4, int(port)))
    
def recv_client():
    while True:
        command, ipv4 = client.recvfrom(1024)
        eval(command.decode('utf-8'))
        
def recv_server():
    while True:
        command, ipv4 = client.recvfrom(1024)
        eval(command.decode('utf-8'))

def main():
        
    thread_client = threading.Thread(target=recv_client)
    thread_server = threading.Thread(target=recv_server)
    thread_client.start()
    thread_server.start()
    
    while True:
        command = input("Enter valid command: ")
        send_to_server(command, manager_ip, manager_port)

if __name__ == '__main__':
    main()