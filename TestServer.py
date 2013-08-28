__author__ = 'h46incon'

import Unity
import PortMapClient
import PortMapServer

import threading
import socketserver
import socket
import time

SERVER_ADDR = ('',PortMapServer.TARGET_ADDR[1])
MSG_LEN = 8 * 1024
END_TAG = b'5f5f'

MSG = bytearray(MSG_LEN)
ID_LEN = 4
HEAD_LEN = 4

PACK_LEN = HEAD_LEN + ID_LEN + MSG_LEN + len(END_TAG)

class Sender(socketserver.StreamRequestHandler):
	def handle(self):
		ID = 0
		pl_b = Unity.UIntToBytes(PACK_LEN, HEAD_LEN)
		while True:
			try:
				ID_B = Unity.UIntToBytes(ID, ID_LEN)

				self.request.sendall(pl_b)
				self.request.sendall(ID_B)
				self.request.sendall(MSG)
				self.request.sendall(END_TAG)
				ID += 1
			except socket.error:
				print('error in socket')


def Client(ip, port):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((ip, port))
	try:
		recv_num = -1
		last_ID = -1
		pack_buf = Unity.PackageJoin(3*MSG_LEN, HEAD_LEN)
		tag_len = len(END_TAG)
		while True:
			buf = sock.recv(2048)
			p_list = pack_buf.Join(buf)
			for p in p_list:
				if len(p) != PACK_LEN - HEAD_LEN:
					print('Len Error')
				ID = Unity.BytesToInt(p[:ID_LEN])
				if ID != last_ID + 1:
					print('ID Error')
				last_ID = ID
				end_tag = p[-tag_len:]
				if end_tag != END_TAG:
					print('Error')
				if ID % 1000 == 0:
					print('Package: ', ID, len(p))
	finally:
		sock.close()

if __name__ == '__main__':
	# Start test server
	server = socketserver.ThreadingTCPServer(SERVER_ADDR,Sender)
	server_thread = threading.Thread(target=server.serve_forever)
	server_thread.setDaemon(True)
	server_thread.start()

	# Start map
	PortMapServer.Main()
	PortMapClient.Main()
	time.sleep(4)

	Client('127.0.0.1', PortMapServer.HOST_PORT)
