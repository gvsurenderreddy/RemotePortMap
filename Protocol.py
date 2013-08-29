import logging
import socket

__author__ = 'h46incon'

import Unity
import abc

STR_ENCODING = 'utf-8'
PORT_ENCODE_LEN = 2
MAX_TCP_PACKAGE_LEN = 8* 1024
ID_LEN = 2
OPT_LEN = 1
ADDR_LEN_LEN = 1
PACKAGE_LEN_LEN = 2

MAIN_CTRL_OPT = Unity.Enum(('new_map', 'map_confirm', 'map_refuse',
							'close_map', 'close_all_map'))
MAP_CTRL_OPT = Unity.Enum(('new_link','link_confirm', 'link_refuse',
						   'close_link', 'close_all_link', 'send_data'))


def PushIDToStream(stream, ID):
	stream.PushInt(ID, ID_LEN)


def ServerShakeHand( _socket):
	'''
	@type _socket: socket.socket
	@type return: bool
	'''
	# Check recv message
	this_port = _socket.getsockname()[1]
	try:
		recv = _socket.recv(1024)
		recv_port = Unity.BytesToInt(recv)

		if recv_port == this_port:
			_socket.sendall(Unity.UIntToBytes(this_port - 1, PORT_ENCODE_LEN))
			return True
		else:
			return False
	except socket.error:
			return False

def ClientShakeHand( _socket):
	'''
	@type _socket: socket.socket
	@type return: bool
	'''
	# Check recv message
	server_port = _socket.getpeername()[1]
	try:
		_socket.sendall(Unity.UIntToBytes(server_port, PORT_ENCODE_LEN))
		recv = _socket.recv(1024)
		recv_port = Unity.BytesToInt(recv)
	except ValueError or socket.error :
		return False
	if recv_port == server_port - 1:
		return True
	else:
		return False


def EncodeAddr(addr, port = None):
	'''
	@addr: IP address, or (IPAddr, port)
	@type port: int
	'''
	if port is None:
		port = addr[1]
		addr = addr[0]
	b = addr.encode(encoding=STR_ENCODING)
	port_b = Unity.UIntToBytes(port, PORT_ENCODE_LEN)
	return b + port_b

def DecodeAddr(byte_msg):
	'''
	@type byte_msg: bytes
	@return (addr, port)
	'''
	addr_b = byte_msg[:-PORT_ENCODE_LEN]
	addr = str(addr_b, encoding = STR_ENCODING)
	port = Unity.BytesToInt(byte_msg[-PORT_ENCODE_LEN:])
	return addr, port


def PackMainCtrlMsg(opt, ID = None, port_msg = None, server_msg = None):
	out_stream = Unity.ByteOutStream(MAX_TCP_PACKAGE_LEN,
									 with_placeholder_for_len = True,
									 place_holder_len= PACKAGE_LEN_LEN)
	# Push operation code
	try:
		out_stream.PushInt(MAIN_CTRL_OPT[opt], OPT_LEN)
	except KeyError:
		raise ValueError('Unrecognizable option:' + opt)
	if opt == 'new_map':
		PushIDToStream(out_stream,ID)
		port_msg_pack = EncodeAddr(port_msg)
		out_stream.PushMsgWithLen(port_msg_pack, ADDR_LEN_LEN)
		server_msg_pack = EncodeAddr(server_msg)
		out_stream.PushMsgWithLen(server_msg_pack, ADDR_LEN_LEN)
	elif opt == 'map_confirm' or opt == 'map_refuse':
		PushIDToStream(out_stream, ID)
	elif opt == 'close_map':
		PushIDToStream(out_stream,ID)
	elif opt == 'close_all_map':
		pass
	else:
		raise ValueError('Unrecognizable option:' + opt)
	return out_stream.GetReadonlyBytes()

def PackMapCtrlMsg(opt, ID = None, msg = None, could_slit_msg = False):
	out_stream = Unity.ByteOutStream(MAX_TCP_PACKAGE_LEN, with_placeholder_for_len = True)
	# Push operation code
	try:
		out_stream.PushInt(MAP_CTRL_OPT[opt], OPT_LEN)
	except KeyError:
		raise ValueError('Unrecognizable option:' + opt)
	if opt == 'new_link' or \
			opt == 'link_confirm' or \
			opt == 'link_refuse'  or \
			opt == 'close_link' :
		PushIDToStream(out_stream, ID)
	elif opt == 'close_all_link':
		pass
	elif opt == 'send_data':
		if could_slit_msg:
			out_list = []

			remain_msg = memoryview(msg)
			remain_len = len(remain_msg)
			out_s = out_stream

			while True:
				PushIDToStream(out_s, ID)

				max_len = min(remain_len, out_s.GetBufRemain())
				out_s.Push(remain_msg[:max_len])
				out_list.append(out_s.GetReadonlyBytes())

				remain_msg = remain_msg[max_len:]
				remain_len = len(remain_msg)
				# Check need continue?
				if remain_len > 0:
					out_s = Unity.ByteOutStream(MAX_TCP_PACKAGE_LEN, with_placeholder_for_len = True)
					out_s.PushInt(MAP_CTRL_OPT[opt], OPT_LEN)
				else:
					break

			return out_list

		else:
			PushIDToStream(out_stream, ID)
			out_stream.Push(msg)
	else:
		raise ValueError('Unrecognizable option:' + opt)
	return out_stream.GetReadonlyBytes()


class BaseMsgUnpacker(metaclass = abc.ABCMeta):
	def __init__(self, package, logger = None):
		self.package = package
		self.in_stream = Unity.ByteInStream(self.package)
		opt_b = self.in_stream.GetMsg(OPT_LEN)
		self.opt = Unity.BytesToInt(opt_b)
		self.logger = logger
		self.has_error = False
		try:
			self._Setup()
		except:
			self._Error('Some errors in package')

	def HasError(self):
		return self.has_error

	@abc.abstractmethod
	def _Setup(self):
		pass

	def _Error(self, msg, *args, **kwargs):
		self.has_error = True
		log_prefix = type(self).__name__ + '\t'
		if self.logger is  not None:
			self.logger.error(log_prefix+msg, *args, **kwargs)

	def _GetID(self):
		id_b = self.in_stream.GetMsg(ID_LEN)
		return Unity.BytesToInt(id_b)

class MainMsgUnpacker(BaseMsgUnpacker):
	def _Setup(self):
		opt = self.opt
		if opt == MAIN_CTRL_OPT['new_map']:
			self.ID = self._GetID()
			# get addr
			addr_b = self.in_stream.GetMsgWithLen(ADDR_LEN_LEN)
			self.port_msg = DecodeAddr(addr_b)
			addr_b = self.in_stream.GetMsgWithLen(ADDR_LEN_LEN)
			self.server_msg = DecodeAddr(addr_b)
		elif opt == MAIN_CTRL_OPT['close_map']:
			self.ID = self._GetID()
		elif opt == MAIN_CTRL_OPT['close_all_map']:
			pass
		else:
			self._Error(
				'Map control received package error, unrecognized operation code: %d', self.opt )

class MapMsgUnpacker(BaseMsgUnpacker):
	def _Setup(self):
		opt = self.opt
		if opt == MAP_CTRL_OPT['new_link'] or \
						opt == MAP_CTRL_OPT['link_confirm'] or \
						opt == MAP_CTRL_OPT['link_refuse']  or \
						opt == MAP_CTRL_OPT['close_link'] :
			self.ID = self._GetID()
		elif opt == MAP_CTRL_OPT['close_all_link']:
			pass
		elif opt == MAP_CTRL_OPT['send_data']:
			self.ID = self._GetID()
			self.msg = self.in_stream.GetAllMsg()
		else:
			self._Error('Unrecognized operation code: %d', opt)


class BaseCtrlHandler(metaclass = abc.ABCMeta):
	global MAX_TCP_PACKAGE_LEN
	recv_buf_len = 16 * 1024
	CONN_OK = 0
	CONN_CLOSE = 1
	CONN_CONNECTING = 2

	@abc.abstractproperty
	def msg_unpacker_class(self):
		return BaseMsgUnpacker

	def __init__(self, _socket, logger = None,data_sender_thread = None):

		self.sub_conn_id_dict = {}
		self.id_sub_conn_dict = {}
		self.connecting_id_dict = {}

		self.socket = _socket
		self.sl = Unity.ThreadingSocketSelect()
		self.logger = logger
		#TODO protocol_handler_dict
		self.protocol_handler_dict = self._SetProtocolHandlerDict()

		if data_sender_thread is None:
			self.data_sender = Unity.ThreadingSocketSender()
		else:
			self.data_sender = data_sender_thread
		self.disconnect_callback = []

		self.has_stopped = False

		self._ListenSelf()

	def Start(self, isDaemon = False):
		if self.has_stopped:
			raise RuntimeError('Could not start ctrl handler which has been stopped before')
		self.sl.setDaemon(isDaemon)
		self.sl.start()
		if not self.data_sender.isAlive():
			self.data_sender.start()

	def Pause(self, join = False, timeout = None):
		self.sl.stop(join, timeout)

	def Stop(self, join = False, timeout = None, inform_other=True):
		self.has_stopped = True
		self.sl.stop(join, timeout)
		self.sl.DeleteSocket(self.socket)
		try:
			self._CloseAllSubConn(inform_other)
		except socket.error:
			pass

	def GetConnState(self, ID):
		if ID in self.id_sub_conn_dict.keys():
			return self.CONN_OK
		if ID in self.connecting_id_dict.keys():
			return self.CONN_CONNECTING
		return self.CONN_CLOSE

	def AddDisconnectCallback(self, callback):
		if callable(callback):
			self.disconnect_callback.append(callback)
		else:
			raise TypeError('The callback must be callable')

	@abc.abstractmethod
	def _SetProtocolHandlerDict(self):
		handler_dict = {}
		return handler_dict

	##################################################
	#Operation of sub connection
	def _AddSubConnInfo(self, connect, ID):
		self.id_sub_conn_dict[ID] = connect
		try:
			old_id = self.sub_conn_id_dict[connect]
		except KeyError:
			self.sub_conn_id_dict[connect] = ID
		else:
			self._Error('The sub connection has assigned an ID:%d', old_id)
		pass

	def _DelSubConnInfo(self, ID):
		try:
			connect = self.id_sub_conn_dict[ID]
			del self.id_sub_conn_dict[ID]
			del self.sub_conn_id_dict[connect]
		except KeyError:
			pass

	def _CloseSubConn(self, ID, inform_other):
		self._Log(logging.INFO, 'Sub connection is closed, ID: %d', ID)
		self._DoCloseSubConnect(ID, inform_other)
		self._DelSubConnInfo(ID)

	@abc.abstractmethod
	def _DoCloseSubConnect(self, ID, inform_other):
		pass

	##################################################
	# Operation of sub connection which is connecting
	def _AddConnecting(self, connect, ID):
		self.connecting_id_dict[ID] = connect

	def _ConnectingConfirm(self, connecting_id, new_conn):
		try:
			self._AddSubConnInfo(new_conn, connecting_id)
			del self.connecting_id_dict[connecting_id]
		except KeyError:
			pass

	def _DelConnectingInfo(self, connecting_id):
		try:
			del self.connecting_id_dict[connecting_id]
		except KeyError:
			pass

	def _CloseConnecting(self, ID, inform_other):
		self._Log(logging.INFO, 'Connecting connection is closed, ID: %d', ID)
		self._DoCloseConnectingConn(ID, inform_other)
		self._DelConnectingInfo(ID)

	@abc.abstractmethod
	def _DoCloseConnectingConn(self, ID, inform_other):
		pass

	##################################################
	# Operation of self
	def _CloseAllSubConn(self, inform_other):
		self._Log(logging.INFO, 'Closing all sub connection' )
		for ID in self.sub_conn_id_dict.keys():
			self._CloseSubConn(ID, inform_other)
		for ID in self.connecting_id_dict.keys():
			self._CloseConnecting(ID, inform_other)

	def _ListenSelf(self):
		pack_buf = Unity.PackageJoin(4 * self.recv_buf_len, PACKAGE_LEN_LEN)
		def SockSelectHandler(_s, pack_buf = pack_buf):
			'''
			@type _s: socket, must equal to self.socket
			'''
			def StopHandler():
				self.Stop(inform_other=False)
				self._RunDisconnectedCallBack()
				return False

			# TODO: How to ensure the handler will be run in serial?
			# Add buffer for each thread
			self._Log(logging.DEBUG - 3, 'Receive message from remote, handling...')
			try:
				buf = _s.recv(self.recv_buf_len)
			except socket.error:
				self._Log(logging.ERROR, 'Self socket has error, stop this handler.')
				return StopHandler()

			if len(buf) == 0:
				self._Log(logging.ERROR, 'Self socket has been closed from other, stop this handler.')
				return StopHandler()

			pack_l = pack_buf.Join(buf)
			for pack in pack_l:
				msg_unpacker = self.msg_unpacker_class(pack, self.logger)
				self._DispatchPackage(msg_unpacker)
			return True

		self.sl.AddSocket(self.socket, SockSelectHandler)
		# self.socket will be closed in Stop()

	def _SelfSendData(self, data):
		self._Log(logging.DEBUG-2, 'Send message to [%s:%d]', *self.socket.getpeername())
		self.data_sender.Add(self.socket, data)

	def _RunDisconnectedCallBack(self):
		# Run disconnect callback
		for cb in self.disconnect_callback:
			cb(self)

	def _Log(self, log_level, msg, *args, **kwargs):
		log_prefix = type(self).__name__ + ' :\t'
		if self.logger is not None:
			self.logger.log(log_level, log_prefix + msg, *args, **kwargs)

	def _Error(self, msg, *args, **kwargs):
		self._Log(logging.ERROR, msg, *args, **kwargs)

	##################################################
	## Handler of message received
	def _DispatchPackage(self, msg_unpacker):
		'''
		@type msg_unpacker: BaseMsgUnpacker
		'''
		try:
			if msg_unpacker.HasError():
				self._Error('Some error in package, ignore it')
				return
			handler = self.protocol_handler_dict[msg_unpacker.opt]
		except KeyError:
			self._Error('Unhand opt: %d', msg_unpacker.opt)
		else:
			if handler is not None:
				try:
					handler(msg_unpacker)
				except AttributeError:
					self._Error('Message unpack or processing error')

	def _CloseSubConnectHandler(self, msg_unpacker):
		ID= msg_unpacker.ID
		if ID in self.connecting_id_dict.keys():
			self._CloseConnecting(ID, inform_other=False)
		elif ID in self.id_sub_conn_dict.keys():
			self._CloseSubConn(ID, inform_other=False)
		self._Log(logging.INFO, 'Sub connection closed from other: %d',ID )

	def _CloseAllSubConnectHandler(self, msg_unpacker):
		self._CloseAllSubConn(inform_other=False)
		self._Log(logging.INFO, 'All sub connection closed from other' )

class BaseMapHandler(BaseCtrlHandler):
	def __init__(self, _socket, target_addr, logger = None, socket_data_sender = None):
		super().__init__(_socket, logger, socket_data_sender)
		self.target_addr = target_addr

	@property
	def msg_unpacker_class(self):
		return MapMsgUnpacker

	def _SendData(self, msg_unpacker):
		socket_id = msg_unpacker.ID
		target_socket = self.id_sub_conn_dict[socket_id]
		if isinstance(target_socket, socket.socket):
			data = msg_unpacker.msg
			self.data_sender.Add(target_socket, data)
			addr = target_socket.getpeername()
			self._Log(logging.DEBUG-1,'Send data from remote to [%s:%d], size: %d',
					addr[0], addr[1], len(data))
		else:
			self._Log(logging.ERROR,'could not sent data to socket, [ID:%d]', socket_id)

	def _DoCloseSubConnect(self, socket_id, inform_other):
		try:
			s = self.id_sub_conn_dict[socket_id]
			if isinstance(s, socket.socket):
				if inform_other:
					self._Log(logging.DEBUG, 'Inform other to close sub link, ID:[%d]', socket_id)
					close_message = PackMapCtrlMsg('close_link', socket_id)
					self._SelfSendData(close_message)
				self.sl.DeleteSocket(s)
				s.shutdown(socket.SHUT_RDWR)
				self._Log(logging.DEBUG, 'Closing socket: [%s:%d]', *s.getpeername())
				s.close()
		except KeyError:
			self._Log(logging.DEBUG, 'the socket ID is not recognized: %d', socket_id)
			pass
		except socket.error:
			pass

	def _HostSockListener(self, _s):
		'''
		@type _s: socket.socket
		'''
		try:
			_s_id = self.sub_conn_id_dict[_s]
		except KeyError:
			self._Log(logging.ERROR,'This socket is not recognized, ignored it. [%s:%d]',
					  *_s.getpeername())
			return False

		try:
			buf = _s.recv(self.recv_buf_len)
		except socket.error:
			self._Log(logging.ERROR,
					  'Some errors do occur in this socket, closed it. [ID %d], [target %s:%d]',
					  _s_id, *_s.getpeername())
			self._CloseSubConn(_s_id, inform_other=True)
			return False

		if len(buf) == 0:
			self._Log(logging.INFO,'This socket has been closed. [ID %d], [target %s:%d]',
					  _s_id, *_s.getpeername())
			self._CloseSubConn(_s_id, inform_other=True)
			return False
		else:
			package_list = PackMapCtrlMsg('send_data', _s_id, buf, could_slit_msg=True)
			for package in package_list:
				self._SelfSendData(package)

			self._Log(logging.DEBUG-1,'Send data from [%s:%d] to remote, size: %d , slit: %d',
					  _s.getpeername()[0], _s.getpeername()[1], len(buf), len(package_list))
			return True

	def _ConnectingConfirm(self, connecting_id, new_conn):
		# Must recode the connection before listen the socket
		super()._ConnectingConfirm(connecting_id, new_conn)
		s = self.id_sub_conn_dict[connecting_id]
		self.sl.AddSocket(s, self._HostSockListener)
		# This socket will be closed when the connection is closed

class BaseMainHandler(BaseCtrlHandler):
	def _DoCloseSubConnect(self, ID, inform_other):
		try:
			map_handler = self.id_sub_conn_dict[ID]
			if inform_other:
				self._Log(logging.DEBUG, 'Inform other to close sub map! ID: %d', ID)
				close_message = PackMainCtrlMsg('close_map', ID)
				self._SelfSendData(close_message)
			if isinstance(map_handler, BaseMapHandler):
				map_handler.Stop()
		except KeyError:
			self._Log(logging.DEBUG, 'ID is unrecognized in _DoCloseSubConnect: %d', ID)
			pass

	def _ConnectingConfirm(self, connecting_id, new_conn):
		# TODO: Consider: Need to close the remote listen socket?
		remote_s = self.connecting_id_dict[connecting_id]
		self.sl.DeleteSocket(remote_s, Unity.ThreadingSocketSelect.LISTEN_READ)
		super()._ConnectingConfirm(connecting_id, new_conn)

	@property
	def msg_unpacker_class(self):
		return MainMsgUnpacker

	def _MapHandleDisconnCallB(self, map_h):
		try:
			map_h_id = self.sub_conn_id_dict[map_h]
			self._DelSubConnInfo(map_h_id)
			self._Log(logging.INFO, 'Map ctrl disconnected: %d', map_h_id)
		except KeyError:
			self._Log(logging.DEBUG, 'The key is not existed in sub_connect_dict.')
