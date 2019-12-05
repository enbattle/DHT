import socket 
import sys  # For sys.argv, sys.exit()

def run():
	if len(sys.argv) != 4:
		print("Error, correct usage is {} [my id] [my port] [k]".format(sys.argv[0]))
		sys.exit(-1)

	#Read command line arguments
	local_id = int(sys.argv[1])
	my_port = str(int(sys.argv[2])) # add_insecure_port() will want a string
	k = int(sys.argv[3])
	my_hostname = socket.gethostname() # Gets my host name
	my_address = socket.gethostbyname(my_hostname) # Gets my IP address from my hostname

	bufferSize = 1024

	UDPServerSocket = socket.socket(family = socket.AF_INET, type = socket.SOCK_DGRAM) 
	UDPServerSocket.bind((my_address, int(my_port))) 

	localIP = "127.0.0.1"
	localPort = 8120
	bufferSize = 1024

	print("This is localIP: {}. This is my_address: {}. Equality: {}".format(localIP, my_address, localIP == my_address))

	UDPServerSocket = socket.socket(family = socket.AF_INET, type = socket.SOCK_DGRAM) 
	UDPServerSocket.bind((localIP, localPort)) 
	print("UDP server up and listening") 

	# this might be database or a file 
	di ={'17BIT0382':'vivek', '17BEC0647':'shikhar', '17BEC0150':'tanveer', 
	'17BCE2119':'sahil', '17BIT0123':'sidhant'} 

	while(True): 
		# receiving name from client 
		name, addr1 = UDPServerSocket.recvfrom(bufferSize) 
		name = name.decode() 
		print("Received name: {}".format(name))
		bytesToSend = str.encode("Ack!") 
		UDPServerSocket.sendto(bytesToSend, addr1) 


		# # receivinf pwd from client 
		# pwd, addr1 = UDPServerSocket.recvfrom(bufferSize) 
		# print("Received pwd: {}".format(pwd))

	# name = name.decode() 
	# pwd = pwd.decode() 
	# msg ='' 
	# if name not in di: 
	# 	msg ='name does not exists'
	# 	flag = 0
	# for i in di: 
	# 	if i == name: 
	# 		if di[i]== pwd: 
	# 			msg ="pwd match"
	# 			flag = 1
	# 		else: 
	# 			msg ="pwd wrong"
	# 	bytesToSend = str.encode(msg) 
	# 	# sending encoded status of name and pwd 
	# 	UDPServerSocket.sendto(bytesToSend, addr1) 

if __name__ == '__main__':
	run()