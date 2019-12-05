import socket 

# user input 
# password = input('enter your password : ') 
# bytesToSend2 = str.encode(password) 

serverAddrPort = ("127.0.1.1", 8120) 
bufferSize = 1024

# conncting to hosts 
UDPClientSocket = socket.socket(family = socket.AF_INET, type = socket.SOCK_DGRAM) 

while(True):
	name = input('enter something : ')	 
	bytesToSend1 = str.encode(name) 
	UDPClientSocket.sendto(bytesToSend1, serverAddrPort) 
	msgFromServer = UDPClientSocket.recvfrom(bufferSize) 
	msg = "Message from Server {}".format(msgFromServer[0].decode()) 
	print(msg)



# print("Sending username")
# # sending username by encoding it 
# print("Sending password")
# # sending password by encoding it 
# UDPClientSocket.sendto(bytesToSend2, serverAddrPort) 

# # receiving status from server 
# print("Blocking on recv")
# msgFromServer = UDPClientSocket.recvfrom(bufferSize) 
# msg = "Message from Server {}".format(msgFromServer[0].decode()) 
# print(msg) 
