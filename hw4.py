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
MAX_BUCKETS = 4 
local_id = None # Uint32 (int)
my_port = None # Uint32 (int)
my_address = None # String
k = None
idkey_idkey = None # Uint32 (int)
k_buckets = []
mode_kv_keyvalue = None # Boolean
mode_kv_keyvalue_key = None # Uint32 (int)
mode_kv_keyvalue_value = None # String 

# class Peer:
#   def __init__(self, id, address, port):
#     self.id = id
#     self.address = address
#     self.port = port

class KadImpl(csci4220_hw4_pb2_grpc.KadImplServicer):
	# Takes an IDKey and returns k nodes with distance closest to ID requested
	def FindNode(self, request, context):
		request_id = request.node.id
		request_address = request.node.address
		request_port = request.node.port

		k_closest_nodes = getKClosestNodesToTargetNode(request.node)

		new_node = csci4220_hw4_pb2.Node(id=request_id, port=request_port, address=request_address)
		storeNodeInKBuckets(new_node)

		k_bucket_str = formatKBucketString()
		print("k_bucket_str:\n" + k_bucket_str)

		return csci4220_hw4_pb2.NodeList(
			responding_node = csci4220_hw4_pb2.Node(
				id = local_id,
				port = int(my_port),
				address = my_address),
			nodes = k_closest_nodes)

	# Takes an IDKey
	# If mode_kv is true, then read value returned by kv
	# Else, read from list of k nodes
	def FindValue(self, request, context):
		return csci4220_hw4_pb2.KV_Node_Wrapper(
			responding_node = csci4220_hw4_pb2.Node(
				id = local_id,
				port = int(my_port),
				address = my_address),
			mode_kv = mode_kv_keyvalue,
			kv = csci4220_hw4_pb2.KeyValue(
				node = csci4220_hw4_pb2.Node(
					id = local_id,
					port = int(my_port),
					address = my_address),
				key = mode_kv_keyvalue_key,
				value = mode_kv_keyvalue_value),
			nodes = [])

	# Takes a KeyValue
	# Stores the value at the given node
	# Needs to return something, but client does not use this return value
	def Store(self, request, context):
		return csci4220_hw4_pb2.IDKey(
			node = csci4220_hw4_pb2.Node(
				id = local_id,
				port = int(my_port),
				address = my_address),
			idkey = idkey_idkey)

	# Takes an IDKey
	# Notifies remote node that the node with the ID in IDKey is quitting the network
	# Remove from the remote node's k-buckets
	# Needs to return something, but client does not use this return value
	def Quit(self, request, context):
		return csci4220_hw4_pb2.IDKey(
			node = csci4220_hw4_pb2.Node(
				id = local_id,
				port = int(my_port),
				address = my_address),
			idkey = idkey_idkey)

def getKClosestNodesToTargetNode(target_node):
	target_id = target_node.id
	k_closest_nodes = []
	k_closest_distances = [] #stores the distance for each of the k closest nodes to the target node
	k_closest_nodes_len = 0 #length of the k_closest_nodes array
	for bucket in k_buckets:
		for node in bucket:
			if(k_closest_nodes_len < k):
				k_closest_nodes.append(node)
				k_closest_distances.append(k_closest_nodes[k_closest_nodes_len].id ^ target_id)
				k_closest_nodes_len += 1
			else:
				current_node_distance = node.id ^ target_id
				#find the greatest distance of the k_closest_nodes
				largest_distance = max(k_closest_distances)
				largest_distance_index = k_closest_distances.index(largest_distance)
				#swap current node with largest node if current node distance < largest node distance
				if(current_node_distance < largest_distance):
					k_closest_nodes[largest_distance_index] = node
					k_closest_distances[largest_distance_index] = current_node_distance

	return k_closest_nodes

#Add the request node to the k_buckets
#Might have to handle a case of fullness (kick something out)
def storeNodeInKBuckets(Node):
	# find the bucket to place the node
	start = 2
	somePower = 0
	result = start**somePower

	xor = local_id ^ Node.id

	# Check if local_id == Node.id
	# Else, perform algorithm
	if xor != 0:
		while result < xor
			somePower+=1
			result = start**somePower

		if result > xor:
			somePower-=1

	# If node is already in list, make sure it is the most recent
	# else, just add it
	found = False
	foundIndex = 0
	for bucket_node in k_buckets[somePower]:
			if bucket_node.id == Node.id:
				found = True
		if found == True:
			k_buckets[somePower].pop(foundIndex) # pop the node
			k_buckets[somePower].append(Node) # re-append the node
		else:
			if(len(k_buckets[somePower]) < k):
				k_buckets[somePower].append(Node) # add node to the end
			else:
				k_buckets[somePower].pop(0) # Pop least recent node
				k_buckets[somePower].append(Node) # add node to the end


def formatKBucketString():
	k_bucket_str = ""
	for i, bucket in enumerate(k_buckets):
		k_bucket_str += str(i) + ": "
		for j, node in enumerate(bucket):
			k_bucket_str += str(bucket[j].id) + ':' + str(bucket[j].port)
			if(j != len(bucket) - 1):
				k_bucket_str += ' '
		if(i != len(k_buckets) - 1):
			k_bucket_str += '\n'
	return k_bucket_str

def handleBootstrapMSG(buffer):
	print("Received BOOTSTRAP command")
	remote_hostname = buffer.split()[1]
	remote_port = buffer.split()[2]
	#send remote node a find_node RPC (defined in the proto file)
	remote_addr = socket.gethostbyname(remote_hostname)
	print("remote_hostname: {}, remote_addr: {}, remote_port: {}".format(remote_hostname, remote_addr, remote_port))
	#need to establish an insecure channel
	channel = grpc.insecure_channel(remote_addr + ':' + remote_port)
	stub = csci4220_hw4_pb2_grpc.KadImplStub(channel)
	response = stub.FindNode(csci4220_hw4_pb2.IDKey(
		node=csci4220_hw4_pb2.Node(id=local_id,port=int(my_port),address=my_address)
		, idkey = local_id))
	print("Just received: id:{} address:{} port:{} nodes:{}".format(
		str(response.responding_node.id), response.responding_node.address, str(response.responding_node.port),
		response.nodes))
	#store remote and each of the k closest nodes in k_buckets
	storeNodeInKBuckets(response.responding_node)
	for node in response.nodes:
		storeNodeInKBuckets(node)
	k_bucket_str = formatKBucketString()
	print("After BOOTSTRAP({}) k_buckets now look like:\n{}".format(response.responding_node.id, k_bucket_str)) 

def handleFindNodeMsg(buffer):
	print("Received FIND_NODE command")
	node_id = int(buffer.split()[1])
	print("node_id " + str(node_id))
	if(local_id == node_id):
		print("Found node!")
	else:
		print("Did not find node. searching...")

def setCommandLineArgs():
	if len(sys.argv) != 4:
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)

	global local_id, my_port, my_address, k

	#Read command line arguments
	local_id = int(sys.argv[1])
	my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
	k = int(sys.argv[3])
	#change back to my_hostname = socket.gethostname() # Gets my host name
	my_hostname = "127.0.0.1"
	my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname

def initializeKBuckets():
	global k_buckets
	k_buckets = [[] for i in range(4)]

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
			handleBootstrapMSG(buffer)
		elif "FIND_NODE" in buffer:
			handleFindNodeMsg(buffer)
		elif "FIND_VALUE" in buffer:
			continue

		elif "STORE" in buffer:
			continue

		elif "QUIT" in buffer:
			continue

		else:
			print("Invalid command! Please try again!")
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
	initializeKBuckets()
	threading.Thread(target=listenForConnections).start()
	blockOnStdin()
