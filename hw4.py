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
import time

#Declaring global variables
_ONE_DAY_IN_SECONDS = 60 * 60 * 24
local_id = None
my_port = None
my_address = None

class KadImpl(csci4220_hw4_pb2_grpc.KadImplServicer):

	def FindNode(self, request, context):
		return csci4220_hw4_pb2.NodeList(
			responding_node=csci4220_hw4_pb2.Node(id=local_id,port=int(my_port),address=my_address),
			nodes=[])

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

def listenForConnections():
	print("gRPC server starting at: {}".format(my_address+':'+my_port))
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
	csci4220_hw4_pb2_grpc.add_KadImplServicer_to_server(KadImpl(), server)
	server.add_insecure_port(my_address + ':' + my_port)
	server.start()
	try:
	    while True:
	        time.sleep(_ONE_DAY_IN_SECONDS)
	except KeyboardInterrupt:
	    server.stop(0)



def blockOnStdin():
	while(True):
		buffer = input('enter something: \n')
		print("MAIN Received from stdin: {}".format(buffer))

		if "BOOTSTRAP" in buffer:
			print("Received BOOTSTRAP command")
			remote_hostname = buffer.split()[1]
			remote_port = buffer.split()[2]
			#send remote node a find_node RPC (defined in the proto file)
			#need to establish an insecure channel
			remote_addr = socket.gethostbyname(remote_hostname)
			print("remote_hostname: {}, remote_addr: {}, remote_port: {}".format(remote_hostname, remote_addr, remote_port))
			channel = grpc.insecure_channel(remote_addr + ':' + remote_port)
			stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
			response = stub.FindNode(csci4220_hw4_pb2.IDKey(
				node=csci4220_hw4_pb2.Node(id=local_id,port=int(my_port),address=my_address)
				, idkey = local_id))
			print("After BOOTSTRAP({}) k_buckets now look like:".format(response.responding_node.id)) 


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
