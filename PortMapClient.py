__author__ = 'h46incon'

import socket
import time
import threading
import logging

import Unity
import Protocol


__SERVER_ADDR = 'localhost'
__SERVER_PORT = 8089

LOG = Unity.SetDefaultLogger()

class ThreadingConnectServer(threading.Thread):
	def __init__(self, addr, handler, shake_hand_method = None,
				   timeout_pre_try = None, try_times = 1,
				   thread_name = None):
		'''
		@addr: (address : port)
		while try_times < 0, then it will try infinitely
		'''
		if thread_name is None:
			thread_name = 'server connecting thread'
		super().__init__(name=thread_name)
		self.connect_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.connect_socket.setblocking(True)
		self.handler = handler
		self.shake_hand_method = shake_hand_method
		self.addr = addr
		self.timeout_pre_try = timeout_pre_try
		self.try_times = try_times
		if try_times < 0:
			self.try_times = None
		self.is_running = False

	def run(self):
		s = None
		addr = self.addr
		self.is_running = True
		# TODO: infinite loop?
		LOG.info('Connecting to server: [%s:%d]', addr[0], addr[1])
		for _ in range(self.try_times):
			if not self.is_running:
				break
			s = self.connect_socket
			if not self.timeout_pre_try is None:
				s.settimeout(self.timeout_pre_try)
			try:
				s.connect(addr)
				# shake hand
				if not self.shake_hand_method is None:
					LOG.info('shaking hand')
					if self.shake_hand_method(s):
						LOG.info('Connect success')
						break
					else:
						s = None
						time.sleep(3)
						LOG.error('_Error in shaking hand')
				else:
					LOG.info('Connect success')
					break
			except socket.timeout:
				s = None
				LOG.error('Connect timeout ')
			except socket.error:
				s = None
				LOG.error('Connect error')
				time.sleep(3)
		if not self.is_running:
			s = None
		self.handler(s)
		self.is_running = False

	def Stop(self, join = False, timeout = None):
		self.is_running = False
		try:
			self.connect_socket.shutdown(socket.SHUT_RDWR)
			self.connect_socket.close()
		except socket.error:
			pass
		if join:
			self.join(timeout)
		pass

class MapHandler(Protocol.BaseMapHandler):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

		target_addr = self.target_addr
		self._Log(logging.INFO, 'Map link start, target [%s:%d], remote address [%s:%d]',
				  target_addr[0],target_addr[1], *self.socket.getpeername())

	def _SetProtocolHandlerDict(self):
		handler_dict = {
			Protocol.MAP_CTRL_OPT['new_link'] : self._NewLink,
			Protocol.MAP_CTRL_OPT['close_link'] : self._CloseSubConnectHandler,
			Protocol.MAP_CTRL_OPT['close_all_link'] : self._CloseAllSubConnectHandler,
			Protocol.MAP_CTRL_OPT['send_data'] : self._SendData,
		}
		return handler_dict

	def _RunDisconnectedCallBack(self):
		#TODO: Add connection information, such as address
		self._Log(logging.INFO,'Map ctrl connection disconnected from the server')
		super()._RunDisconnectedCallBack()

	def _DoCloseConnectingConn(self, ID, inform_other):
		try:
			t = self.connecting_id_dict[ID]
			if isinstance(t, ThreadingConnectServer):
				# The socket is connecting, stop the thread
				t.Stop()
				if inform_other:
					msg = Protocol.PackMapCtrlMsg('link_refuse', ID)
					self._SelfSendData(msg)
		except KeyError:
			self._Log(logging.DEBUG, 'The key is not existed in conneting_id_dict: %d', ID)


	def _NewLink(self, msg_unpacker):
		socket_id = msg_unpacker.ID
		def ConnectedCallback(_socket):
			if not _socket is None:
				self._ConnectingConfirm(socket_id, _socket)
				confirm_message = Protocol.PackMapCtrlMsg('link_confirm', socket_id)
				self._SelfSendData(confirm_message)
				self._Log(logging.DEBUG, 'Sending confirm message to remote, ID: [%d]', socket_id)
			else:
				try:
					refuse_message = Protocol.PackMapCtrlMsg('link_refuse', socket_id)
					self._SelfSendData(refuse_message)
					self._DelConnectingInfo(socket_id)
					self._Log(logging.DEBUG, 'Sending refuse message to remote, ID: [%d]', socket_id)
				except socket.error:
					pass

		thread = ThreadingConnectServer(
			addr=self.target_addr,
			handler=ConnectedCallback,
			shake_hand_method=None,
			try_times = 100)

		self._Log(logging.INFO, 'New link request, ID: %d, target [%s:%d]', socket_id, *self.target_addr)
		self._AddConnecting(thread, socket_id)
		thread.start()

class MainHandler(Protocol.BaseMainHandler):
	def _SetProtocolHandlerDict(self):
		handler_dict = {
				Protocol.MAIN_CTRL_OPT['new_map']: self._NewMap,
				Protocol.MAIN_CTRL_OPT['close_map']: self._CloseSubConnectHandler,
				Protocol.MAIN_CTRL_OPT['close_all_map']: self._CloseAllSubConnectHandler
			}
		return handler_dict

	def _RunDisconnectedCallBack(self):
		self._Log(logging.INFO,'Main ctrl connection disconnected from the server')
		super()._RunDisconnectedCallBack()

	def _DoCloseConnectingConn(self, ID, inform_other):
		try:
			t = self.connecting_id_dict[ID]
			if isinstance(t, ThreadingConnectServer):
				# The socket is connecting, stop the thread
				t.Stop()
		except KeyError:
			self._Log(logging.DEBUG, 'The key is not existed in conneting_id_dict: %d', ID)

	def _NewMap(self, msg_unpacker):
		socket_id = msg_unpacker.ID
		def ConnectedCallback(_socket, msg_unpacker = msg_unpacker):
			self._Log(logging.DEBUG, 'Map connect successed! ID: %d', socket_id)
			if not _socket is None:
				map_ctrl = MapHandler(
					_socket=_socket,
					target_addr=msg_unpacker.port_msg,
					logger=self.logger,
					socket_data_sender=self.data_sender
				)
				map_ctrl.AddDisconnectCallback(self._MapHandleDisconnCallB)
				self._ConnectingConfirm(socket_id, map_ctrl)
				map_ctrl.Start()
			else:
				self._DelConnectingInfo(socket_id)

		thread = ThreadingConnectServer(
			addr=msg_unpacker.server_msg,
			handler=ConnectedCallback,
			shake_hand_method=Protocol.ClientShakeHand,
			try_times = 100)

		self._Log(logging.INFO, 'New map request, ID: %d, target [%s:%d]', socket_id, *msg_unpacker.port_msg)
		self._AddConnecting(thread, socket_id)
		thread.start()

if __name__ == '__main__':
	def handler(_s):
		if not _s is None:
			h = MainHandler(_s, logger=LOG)
			h.Start(isDaemon = False)
	ThreadingConnectServer((__SERVER_ADDR,__SERVER_PORT),
						   handler,
						   Protocol.ClientShakeHand,
						   try_times = 999999).start()
