#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import select
import queue as Queue
import grpc

import csci4220_hw4_pb2
import csci4220_hw4_pb2_grpc
import threading 

#Declaring global variables
local_id = None
my_port = None
my_address = None

def setCommandLineArgs():
	if len(sys.argv) != 4:
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)

	global local_id, my_port, my_address

	#Read command line arguments
	local_id = int(sys.argv[1])
	my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
	k = int(sys.argv[3])
	#change back to my_hostname = socket.gethostname() # Gets my host name
	my_hostname = "127.0.0.1"
	my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname
	print("My address: {}".format(my_address))

def listenForConnections():
	UDPServerSocket = socket.socket(family = socket.AF_INET, type = socket.SOCK_DGRAM) 
	UDPServerSocket.bind((my_address, int(my_port))) 
	# print("Listening for datagrams on {}: {}".format(my_address,my_port))
	print("THREAD About to block on recv")
	while(True): 
		# receiving name from client 
		name, addr1 = UDPServerSocket.recvfrom(bufferSize) 
		name = name.decode() 
		print("THREAD Received from client: {}".format(name))
		bytesToSend = str.encode("Ack!") 
		UDPServerSocket.sendto(bytesToSend, addr1) 


def blockOnStdin():
	while(True):
		buffer = input('enter something: \n')
		print("MAIN Received from stdin: {}".format(buffer))

		if "BOOTSTRAP" in buffer:
			print("Received BOOTSTRAP command")
			remote_hostname = buffer.split()[1]
			remote_port = buffer.split()[2]
			print("remote_hostname: {}, remote_port: {}".format(remote_hostname, remote_port))
			#send remote node a find_node RPC (defined in the proto file)
			#need to establish an insecure channel
			remote_addr = socket.gethostbyname(remote_hostname)
			channel = grpc.insecure_channel(remote_addr + ':' + remote_port)
			stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
			# response = stub.FindNode(helloworld_pb2.HelloRequest(name='you'))


	''' Use the following code to convert a hostname to an IP and start a channel
	Note that every stub needs a channel attached to it
	When you are done with a channel you should call .close() on the channel.
	Submitty may kill your program if you have too many file descriptors open
	at the same time. '''
	
	#remote_addr = socket.gethostbyname(remote_addr_string)
	#remote_port = int(remote_port_string)
	#channel = grpc.insecure_channel(remote_addr + ':' + str(remote_port))

if __name__ == '__main__':
	bufferSize = 1024
	setCommandLineArgs()
	threading.Thread(target=listenForConnections).start()
	blockOnStdin()
