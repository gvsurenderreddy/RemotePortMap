__author__ = 'h46incon'

import Unity
import Protocol

import socket
import time
import threading
import logging

REMOTE_LISTEN_PORT = 8089
HOST_PORT = 8695
TARGET_ADDR = ('localhost', 9536)


LOG = Unity.SetDefaultLogger()
BACK_LOG = 5

def ServerShakeHandWrap(_socket, callback):
	c_addr, c_port = _socket.getpeername()
	LOG.info('Connected from [%s:%d], shaking hand...', c_addr, c_port)
	if Protocol.ServerShakeHand(_socket):
		LOG.info('Connected success!')
		callback(_socket)
	else:
		LOG.info('Connected failed!')
		try:
			_socket.shutdown(socket.SHUT_RDWR)
			_socket.close()
		except socket.error:
			pass
		callback(None)

def ThreadingServerShakeHand(callback, shake_in_new_thread = True):
	def SockSelectCallBack(_s):
		'''
		@type _s: socket.socket
		'''
		client, address = _s.accept()
		if shake_in_new_thread:
			thread = threading.Thread(target=ServerShakeHandWrap, args=(client,callback))
			thread.start()
		else:
			ServerShakeHandWrap(client, callback)
		# still listenning this socket
		return True

	return SockSelectCallBack

class MapHandler(Protocol.BaseMapHandler):
	def __init__(self, _socket, host_socket, target_addr,
				 logger = None, socket_data_sender = None):
		super().__init__(
			_socket=_socket,
			target_addr=target_addr,
			logger=logger,
			socket_data_sender=socket_data_sender
		)

		max_ID = 2 ** (8 * Protocol.ID_LEN) - 1
		self.id_set = Unity.IDGenerator(max_ID)

		# listen host
		self._Log(logging.INFO, 'Map link start!, host port [%d], target [%s:%d], remote address [%s:%d]',
				  host_socket.getsockname()[1], target_addr[0],target_addr[1], *self.socket.getpeername())
		self.host_socket = host_socket
		self.host_socket.listen( BACK_LOG )
		self.sl.AddSocket(self.host_socket,
						 self._HostNewConnectHandler
						)
		# The host_socket will be closed in Stop()

	def Stop(self, join = False, timeout = None, inform_other=True):
		self.sl.DeleteSocket(self.host_socket)
		super().Stop(join, timeout, inform_other)

	def _SetProtocolHandlerDict(self):
		handler_dict = {
			Protocol.MAP_CTRL_OPT['link_confirm'] : self._LinkConfirm,
			Protocol.MAP_CTRL_OPT['link_refuse'] : self._LinkRefuse,
			Protocol.MAP_CTRL_OPT['close_link'] : self._CloseSubConnectHandler,
			Protocol.MAP_CTRL_OPT['close_all_link'] : self._CloseAllSubConnectHandler,
			Protocol.MAP_CTRL_OPT['send_data'] : self._SendData,
		}
		return handler_dict

	def _RunDisconnectedCallBack(self):
		#TODO: Add connection information, such as address
		LOG.info('Map ctrl connection disconnected from the server')
		super()._RunDisconnectedCallBack()

	def _DelSubConnInfo(self, ID):
		super()._DelSubConnInfo(ID)
		self.id_set.Del(ID)

	def _DoCloseConnectingConn(self, ID, inform_other):
		try:
			s = self.connecting_id_dict[ID]
			s.shutdown(socket.SHUT_RDWR)
			s.close()
		except socket.error:
			pass
		except KeyError:
			self._Log(logging.DEBUG, 'The key is not existed in conneting_id_dict: %d', ID)

	def _LinkConfirm(self, msg_unpacker):
		try:
			ID = msg_unpacker.ID
			s = self.connecting_id_dict[ID]
			self._ConnectingConfirm(ID, s)
			self._Log(logging.INFO, 'link confirm, ID: %d', ID)
		except KeyError:
			self._Error('Unrecognized ID in a link confirm package: %d', ID)

	def _LinkRefuse(self, msg_unpacker):
		try:
			ID = msg_unpacker.ID
			self._DelConnectingInfo(ID)
			s = self.connecting_id_dict[ID]
			s.shutdown(socket.SHUT_RDWR)
			s.close()
			self.id_set.Del(ID)
			self._Log(logging.ERROR, 'link refuse, ID: %d', ID)
		except KeyError:
			self._Error('Unrecognized ID in a link refuse package: %d', ID)
		except socket.error:
			pass

	def _HostNewConnectHandler(self, _socket):
		client, address = _socket.accept()
		new_id = self.id_set.Gen()
		self._Log(logging.INFO, 'New connected in map host port. allocated ID [%d], address [%s:%d]',
				  new_id, *address)
		new_link_msg = Protocol.PackMapCtrlMsg('new_link', new_id)
		self._AddConnecting(client, new_id)
		self._SelfSendData(new_link_msg)
		# Now waiting for new link confirm message
		# And still listen this socket
		return True

class MainHandler(Protocol.BaseMainHandler):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		max_ID = 2 ** (8 * Protocol.ID_LEN) - 1
		self.id_set = Unity.IDGenerator(max_ID)
		self.id_remote_sock = {}

	def _SetProtocolHandlerDict(self):
		handler_dict = {
			Protocol.MAIN_CTRL_OPT['close_map']: self._CloseSubConnectHandler,
			Protocol.MAIN_CTRL_OPT['close_all_map']: self._CloseAllSubConnectHandler
		}
		return handler_dict

	def _DelSubConnInfo(self, ID):
		super()._DelSubConnInfo(ID)
		self.id_set.Del(ID)

	def _DoCloseConnectingConn(self, ID, inform_other):
		try:
			s = self.connecting_id_dict[ID]
			s.shutdown(socket.SHUT_RDWR)
			s.close()
		except KeyError:
			self._Log(logging.DEBUG, 'The ID is not existed: %d', ID)
		except socket.error:
			pass

	def NewMap(self, target_addr, host_addr, remote_listen_addr = None, host_reuse_addr = False):

		host_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		if host_reuse_addr:
			host_s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		host_s.bind(host_addr)
		new_id = self.id_set.Gen()

		def RemoteMapConnectHandler(_s, ID = new_id, host_s = host_s, target_addr = target_addr):
			if not _s is None:
				map_h = MapHandler(
					_socket=_s,
					host_socket=host_s,
					target_addr=target_addr,
					logger=self.logger,
					socket_data_sender=self.data_sender
				)
				self._ConnectingConfirm(ID, map_h)
				map_h.AddDisconnectCallback(self._MapHandleDisconnCallB)
				map_h.Start()
			else:
				#self._DelConnectingInfo(ID)
				#self.id_set.Del(ID)
				# Still waiting for connection
				pass

		if remote_listen_addr is None:
			#bind a random port
			remote_listen_addr = ('',0)
		remote_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		remote_s.bind(remote_listen_addr)
		remote_listen_addr = remote_s.getsockname()
		self._Log(logging.INFO, 'Remote listen addr: [%s:%d]', remote_listen_addr[0],remote_listen_addr[1])
		# listen this socket
		remote_s.listen( BACK_LOG )
		self._AddConnecting(remote_s, new_id)
		self.id_remote_sock[new_id]	 = remote_s

		self.sl.AddSocket(remote_s,
						  ThreadingServerShakeHand(RemoteMapConnectHandler)
						  )
		# this remote_s will be closed when map connection confirm in _ConnectingConfirm
		# Send new map message
		msg = Protocol.PackMainCtrlMsg(opt = 'new_map', ID = new_id, port_msg = target_addr, server_msg=remote_listen_addr)
		self._SelfSendData(msg)
		return new_id

	def CloseMap(self, ID):
		if ID in self.id_sub_conn_dict.keys():
			self._CloseSubConn(ID, inform_other=True)
		elif ID in self.connecting_id_dict.keys():
			self._CloseConnecting(ID, inform_other=True)
		else:
			self._Log(logging.DEBUG, 'the ID is not existed: %d', ID)

	def CloseAllMap(self):
		self._CloseAllSubConn(inform_other=True)

	def _RunDisconnectedCallBack(self):
		#TODO: Add connection information, such as address
		LOG.info('Main ctrl connection disconnected from the server')


def Main():
	def MainHandleConnected(_s):
		if not _s is None:
			main_h = MainHandler(
				_socket=_s,
				logger=LOG
				)
			main_h.Start()

			LOG.info('setting new map...')
			map_ID = main_h.NewMap(
				target_addr = TARGET_ADDR,
				host_addr = ('',HOST_PORT),
				host_reuse_addr = True,
				remote_listen_addr=('localhost',0)
			)
			while True:
				state = main_h.GetConnState(map_ID)
				if state == main_h.CONN_OK:
					LOG.info('new map OK')
					break
				elif state == main_h.CONN_CONNECTING:
					LOG.info('new map waiting...')
					time.sleep(3)
				elif state == main_h.CONN_CLOSE:
					LOG.info('new map closed')
					return
			#main_h.CloseMap(map_ID)
			#time.sleep(3)
			#main_h.CloseAllMap()


	main_h_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	main_h_s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
	main_h_s.bind(('',REMOTE_LISTEN_PORT))
	main_h_s.listen( BACK_LOG )
	LOG.info('listen begin...')
	sl = Unity.ThreadingSocketSelect()
	sl.AddSocket(main_h_s,
				 ThreadingServerShakeHand(MainHandleConnected, shake_in_new_thread=False)
				)
	sl.start()
	# This socket will never stop listening...

if __name__ == '__main__':
	Main()

