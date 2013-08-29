__author__ = 'h46incon'

import Unity
import PortMapClient
import PortMapServer

import threading
import multiprocessing
import socketserver
import socket
import time

SERVER_ADDR = ('',PortMapServer.TARGET_ADDR[1])
MSG_LEN = 8 * 1024
END_TAG = b'5'

MSG = bytearray(MSG_LEN)
ID_LEN = 4
HEAD_LEN = 4

PACK_LEN = HEAD_LEN + ID_LEN + MSG_LEN + len(END_TAG)
PACK_NUM = 1

THREAD_NUM = 256
BUF_LEN = 1024 * 5

SERVER_SEND = True

class ThreadingTCPServer(socketserver.ThreadingTCPServer):
	allow_reuse_address = True

def SendMsg(sock):
	ID = 0
	pl_b = Unity.UIntToBytes(PACK_LEN, HEAD_LEN)
	for _ in range(PACK_NUM):
		try:
			ID_B = Unity.UIntToBytes(ID, ID_LEN)

			sock.sendall(pl_b)
			sock.sendall(ID_B)
			sock.sendall(MSG)
			sock.sendall(END_TAG)
			ID += 1
			#nbytes = sock.recv_into(bytebuf, BUF_LEN)
		except socket.error:
			print('error in socket')

def RecvMsg(sock):
	last_ID = -1
	pack_buf = Unity.PackageJoin(2 * BUF_LEN, HEAD_LEN)
	tag_len = len(END_TAG)
	while True:
		buf = sock.recv(BUF_LEN)
		if len(buf) == 0:
			return
		p_list = pack_buf.Join(buf)
		for p in p_list:
			if len(p) != PACK_LEN - HEAD_LEN:
				print('Len Error', len(p))
				raise RuntimeError('STOP')
			ID = Unity.BytesToInt(p[:ID_LEN])
			if ID != last_ID + 1:
				print('ID Error', last_ID, ID)
				ID = last_ID + 1
				raise RuntimeError('STOP')
			last_ID = ID
			end_tag = p[-tag_len:]
			if end_tag != END_TAG:
				print('Error')
			#sock.sendall(buf)

class Sender(socketserver.StreamRequestHandler):
	def handle(self):
		if SERVER_SEND:
			SendMsg(self.request)
		else:
			RecvMsg(self.request)


def Client(ip, port, CID):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))
	try:
		if SERVER_SEND:
			RecvMsg(sock)
		else:
			SendMsg(sock)
	except RuntimeError:
		print('-----------------------Error in:', CID)
	finally:
		print('Package: ', CID)
		sock.close()

if __name__ == '__main__':
	# Start test server
	server = ThreadingTCPServer(SERVER_ADDR,Sender)
	server_thread = threading.Thread(target=server.serve_forever)
	server_thread.start()

	# Start map
	p = multiprocessing.Process(target=PortMapServer.Main)
	p.start()
	p = multiprocessing.Process(target=PortMapClient.Main)
	p.start()
	time.sleep(2)


	#print('start')
	#for i in range(THREAD_NUM):
	#	client_thread = threading.Thread(target=Client,
	#						args=('localhost', PortMapServer.HOST_PORT, i))
	#						#args=('localhost', SERVER_ADDR[1], i))
	#	client_thread.setDaemon(False)
	#	client_thread.start()
	#	#time.sleep(0.1)
