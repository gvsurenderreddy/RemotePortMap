__author__ = 'h46incon'

import logging
import select
import socket
import time
import threading
import queue

INT_BYTE_ORDER = 'big'
INT_ENCODE_LEN = 2

#Debug
#def memoryview(b):
#	return b

def CheckParaType(_para, _type):
	if not isinstance(_para, _type):
		raise TypeError('The para must be ' + _type.__name__)

def Enum(name_list):
	v_range = range(len(name_list))
	v_iter = iter(v_range)
	return {name:next(v_iter) for name in name_list }

def SetDefaultLogger(logger, log_lev = logging.DEBUG -2):
	'''
	@return: logger
	'''
	log_format = logging.Formatter("%(asctime)s  %(levelname)s - %(message)s","%Y-%m-%d %H:%M:%S")
	l_handler = logging.StreamHandler()
	l_handler.setFormatter(log_format)
	logger.setLevel(log_lev)
	logger.addHandler(l_handler)

def UIntToBytes(int_v, bytes_len = INT_ENCODE_LEN):
	'''
	@int_v: int value to encode
	@type int_v: int
	'''
	if not 0 <= int_v < 2 ** (8 * bytes_len):
		raise ValueError('The para\'s value must between 0 and %d', 2 ** (8*bytes_len))
	return int_v.to_bytes(bytes_len, INT_BYTE_ORDER)

def BytesToInt(d_bytes):
	'''
	@type d_bytes: bytes
	'''
	return int.from_bytes(d_bytes, INT_BYTE_ORDER)

def Range(end = None):
	'''
	This range could be infinite when end is None
	'''
	if end is None:
		counter = 0
		while True:
			yield  counter
			counter +=1
	else:
		return range(end)

class ByteOutBuffer:
	def __init__(self, buflen):
		self.buflen = buflen
		self.bytebuf = bytearray(buflen)
		self.len = 0

	def Set(self, bytes_to_write, begin_pos):
		end_pos = begin_pos + len(bytes_to_write)
		if end_pos > self.buflen:
			raise MemoryError('Not enough space in buffer')
		self.bytebuf[begin_pos: end_pos] = bytes_to_write
		if end_pos > self.len :
			self.len = end_pos

	def Push(self, bytes_to_push):
		self.Set(bytes_to_push, self.len)

	def GetReadonlyBytes(self):
		return memoryview(self.bytebuf)[0:self.len]

class ByteOutStream(ByteOutBuffer):
	global INT_ENCODE_LEN
	def __init__(self, buflen, with_placeholder_for_len = False, place_holder_len = INT_ENCODE_LEN):
		super().__init__(buflen)
		if with_placeholder_for_len:
			self.place_holder_len = place_holder_len
			self.PushInt(0, place_holder_len)
		else:
			self.place_holder_len = 0

	def GetBufRemain(self):
		return self.buflen - self.len

	def PushInt(self, int_v, bytes_len):
		b = UIntToBytes(int_v, bytes_len)
		self.Push(b)

	def PushMsgWithLen(self, msg, len_len ):
		msg_len = len(msg)
		self.PushInt(msg_len, len_len)
		self.Push(msg)

	def GetReadonlyBytes(self):
		if self.place_holder_len != 0:
			len_b = UIntToBytes(self.len, self.place_holder_len)
			self.Set(len_b, 0)
		return super(type(self), self).GetReadonlyBytes()

class ByteInStream:
	global INT_ENCODE_LEN
	def __init__(self, bytes_stream):
		'''
		@type bytes_stream: bytes
		'''
		self.bytes_stream = bytes_stream
		self.index = 0

	def GetMsg(self, msg_len):
		end = self.index + msg_len
		if end > len(self.bytes_stream):
			raise IndexError('Not enough bytes')
		msg = memoryview(self.bytes_stream)[self.index:end]
		self.index = end
		return msg

	def GetAllMsg(self):
		msg_len = len(self.bytes_stream) - self.index
		return self.GetMsg(msg_len)

	def GetMsgWithLen(self, len_len):
		len_b = self.GetMsg(len_len)
		msg_len = BytesToInt(len_b)
		return self.GetMsg(msg_len)

class PackageJoin:
	def __init__(self, buf_len, head_len):
		self.buf_len = buf_len
		self.buf = bytearray(buf_len)
		self.buf_windows = memoryview(self.buf)
		self.data_len = 0

		self.head_len = head_len

		self.next_pack_len = None
		self.next_pack = b''

	def __TryGetMsg(self, msg_len):
		'''
		Look is there any bytes in buffer
		If any, then copy enough bytes from packet to buffer to construct an unbroken message
		Else, try to construct an unbroken message from package directly
		If could not construct an unbroken message, copy whole package to buffer
		'''
		if self.data_len == 0:
			# Try Get message from package directly
			if len(self.next_pack) >= msg_len:
				return self.__PopFrontOfNextPack(msg_len)
			else:
				# Not enough bytes, copy package to buffer
				if len(self.next_pack) != 0:
					self.__PushPackageToBuffer()
				return None
		else:
			# Try push enough bytes to buffer first
			need_push_len = msg_len - self.data_len
			if self.__PushPackageToBuffer(need_push_len):
				return self.__PopFrontOfBuff(msg_len)
			else:
				return None

	def __PopFrontOfNextPack(self, pop_len = None):
		if pop_len is None:
			pop_len = len(self.next_pack)
		msg = self.next_pack[:pop_len]
		self.next_pack = self.next_pack[pop_len:]
		if len(self.next_pack) == 0:
			# Release the reference
			self.next_pack = b''
		return msg

	def __PopFrontOfBuff(self, pop_len = None):
		'''
		if pop_len is None, pop whole buffer
		'''
		if pop_len is None:
			pop_len = self.data_len
		# make a copy in msg
		msg = self.buf_windows[0:pop_len].tobytes()
		self.data_len -= pop_len
		if self.data_len == 0:
			# Move buf_windows to the begin of buffer
			self.buf_windows = memoryview(self.buf)
		else:
			self.buf_windows = self.buf_windows[pop_len:]

		# just because need to return a memoryview object
		return memoryview(msg)

	def __PushPackageToBuffer(self, msg_len = None):
		'''
		If msg_len = None, then push whole package to buffer
		Try push msg_len bytes from self.next_pack to buffer
		@return: True if push enough bytes, False if push less than msg_len
		'''
		# Check is there enough space in the back
		if msg_len is None:
			msg_len = len(self.next_pack)
		push_len = min(msg_len, len(self.next_pack))

		if push_len <= 0:
			return True

		if push_len + self.data_len > self.buf_len:
			raise MemoryError('Not enough memory in buffer')

		if push_len + self.data_len > len(self.buf_windows):
			# Move buf_windows to the head of buf
			self.buf[0:self.data_len] = self.buf_windows[0:self.data_len]
			self.buf_windows = memoryview(self.buf)

		begin_i = self.data_len
		self.buf_windows[begin_i:begin_i+push_len] = self.__PopFrontOfNextPack(push_len)
		self.data_len += push_len
		return push_len == msg_len

	def Join(self, package):
		'''
		@return list of memoryview of unbroken message
		'''
		if len(self.next_pack) != 0:
			# This code should not be run
			self.__PushPackageToBuffer()
			raise RuntimeError('The last package must be handled')
		self.next_pack = memoryview(package)
		msg_list = []
		while True:
			# Get len first
			if self.next_pack_len is None:
				len_b = self.__TryGetMsg(self.head_len)
				if len_b is None:
					break
				else:
					self.next_pack_len = BytesToInt(len_b) - self.head_len

			msg = self.__TryGetMsg(self.next_pack_len)
			if msg is not None:
				self.next_pack_len = None
				msg_list.append(msg)
			else:
				break
		return msg_list

class IDGenerator:
	def __init__(self, max_ID = None):
		self.max_ID = max_ID
		self.last_ID = 0	# ID 0 is skipped in the first times
		self.ID_set = set()

	def Gen(self):
		try_id = self.last_ID + 1
		try_loop_times = 2	# first times is from last_ID to max_ID
							# second times is from 0 to max_ID
		for _ in range(try_loop_times):
			while self.max_ID is None or try_id <= self.max_ID:
				if not try_id in self.ID_set:
					self.ID_set.add(try_id)
					self.last_ID = try_id
					return try_id
				else:
					try_id += 1
			try_id = 0		# try from 0

		raise GeneratorExit('could not generate an ID, because the ID set is full')


	def Del(self, ID):
		try:
			self.ID_set.remove(ID)
		except KeyError:
			pass

class ThreadingSocketSender(threading.Thread):
	__AWAKEN_ITEM = 'awaken_item'
	def __init__(self, name = 'Socket sender thread'):
		super().__init__(name=name)
		self.queue_obj = queue.Queue()
		self.is_running = False

	def run(self):
		self.is_running = True
		while self.is_running:
			item = self.queue_obj.get(block=True)
			if item != self.__AWAKEN_ITEM:
				_socket = item[0]
				_data = item[1]
				try:
					_socket.sendall(_data)
				except socket.error:
					#TODO: log?
					pass

			self.queue_obj.task_done()
		self.is_running = False

	def Add(self, socket, data):
		self.queue_obj.put((socket, data))

	def Stop(self):
		self.is_running = False
		# Active the running thread
		if self.queue_obj.empty():
			self.queue_obj.put(item=self.__AWAKEN_ITEM)

class ThreadingSocketSelect(threading.Thread):
	'''
	Wrap for the select.select(), it will run in a new thread
	'''

	@staticmethod
	def SleepAndContinue(sleep_seconds):
		def Sleep(_ = None):
			time.sleep(sleep_seconds)
			return True
		return Sleep

	DEFAULT_SLOT_SECOND = 2
	SLEEP_AND_CONTINUE = SleepAndContinue.__func__(2)
	LISTEN_READ = 0
	LISTEN_WRITE = 1
	LISTEN_ERROR = 2

	def __init__(self, poll_interval = DEFAULT_SLOT_SECOND, Idle_handle = SLEEP_AND_CONTINUE):
		super().__init__(name= 'socket selecting thread')
		self.sock_handler_dic_list = [{},{},{}]
		self.working = False
		self.poll_interval = poll_interval
		self.Idle_handle = Idle_handle

	def AddSocket(self, socket, handler, listen_type = LISTEN_READ):
		'''
		@handler: a function : bool handler(socket).
				which receive a socket as parameter
				and return a bool flag
					True for list this socket continue, False for ignore the socket in future
		'''
		self.sock_handler_dic_list[listen_type][socket] = handler

	def DeleteSocket(self, socket, listen_type = LISTEN_READ):
		try:
			del self.sock_handler_dic_list[listen_type][socket]
		except KeyError:
			pass

	def run(self):
		self.working = True
		handler_dict_list = self.sock_handler_dic_list
		while self.working:
			# Test if is idle
			working_set = [True for i in handler_dict_list if i]
			if working_set:
				try:
					socket_l_l= select.select(
						handler_dict_list[self.LISTEN_READ].keys(),
						handler_dict_list[self.LISTEN_WRITE].keys(),
						handler_dict_list[self.LISTEN_ERROR].keys(),
						self.poll_interval)
				except OSError:
					# while handler_dict_list is empty, it will throw a error
					# TODO
					socket_l_l = [[],[],[]]
					pass

				for (socket_list,handler_dict) in zip(socket_l_l, handler_dict_list):
					for s in socket_list:
						try:
							handler = handler_dict[s]
							if handler is not None:
								if not handler(s):
									# TODO: log?
									handler_dict.pop(s)
						except KeyError:
							pass
			else:
				# IDLE
				if self.Idle_handle is not None:
					if not self.Idle_handle(self):
						break
				else:
					break
		self.working = False

	def stop(self, join = False, time_out = None):
		self.working = False
		if join:
			self.join(time_out)


