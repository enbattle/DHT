#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()

import select
import queue as Queue
import grpc

import csci4220_hw4_pb2
import csci4220_hw4_pb2_grpc

def run():
	if len(sys.argv) != 4:
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)

	local_id = int(sys.argv[1])
	my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
	k = int(sys.argv[3])
	my_hostname = socket.gethostname() # Gets my host name
	my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname

	# creating the server socket
	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)     
	server.setblocking(0)     
	server.bind((my_hostname, int(my_port)))         
	server.listen(10)
	inputs = [server]
	outputs = []
	message_queues = {}

	# creating the client socket
	client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	print("Server started on port:", my_port)

	# Infinite loop for selecting connections
	while inputs: 
		# check if sockets are ready to write, read, or some error/exception occurred
		# blocks until a passed socket is ready
		readable, writable, exception = select.select(inputs, outputs, inputs)

		# Loop for the ready sockets in the readable list
		# List for new incoming clients
		for ready_socket in readable:
			if ready_socket is server:
				connection, client_address = ready_socket.accept()
				connection.setblocking(0)
				inputs.append(connection)
				message_queues[connection] = Queue.Queue()
			else:
				data = ready_socket.recv(1024)
				if data:
					message_queues[ready_socket].put(data)
					if ready_socket not in outputs:
						outputs.append(ready_socket)
				else:
					if ready_socket in outputs:
						outputs.remove(ready_socket)
					inputs.remove(ready_socket)
					ready_socket.close()
					del message_queues[ready_socket]

		# Loop for the ready sockets in the writable list
		# Get pending messages and writes them to the socket
		for ready_socket in writable:
			try:
				message = message_queues[ready_socket].get_nowait()
			except Queue.Empty:
				outputs.remove(ready_socket)
			else:
				ready_socket.send(message)

		# Loop for the sockets that have errors in the exception list
		# If there are any errors, the socket is removed from the lists
		for ready_socket in exception:
			if ready_socket in outputs:
				outputs.remove(ready_socket)
			inputs.remove(ready_socket)
			ready_socket.close()
			del message_queues[ready_socket]


	''' Use the following code to convert a hostname to an IP and start a channel
	Note that every stub needs a channel attached to it
	When you are done with a channel you should call .close() on the channel.
	Submitty may kill your program if you have too many file descriptors open
	at the same time. '''
	
	#remote_addr = socket.gethostbyname(remote_addr_string)
	#remote_port = int(remote_port_string)
	#channel = grpc.insecure_channel(remote_addr + ':' + str(remote_port))

if __name__ == '__main__':
	run()
