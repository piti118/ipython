import sys
import time
import os
import zmq
from threading import Lock, Thread, current_thread
from io import StringIO

from session import extract_header, Message

from IPython.utils import io, text
from IPython.utils import py3compat

#-----------------------------------------------------------------------------
# Globals
#-----------------------------------------------------------------------------

#-----------------------------------------------------------------------------
# Stream classes
#-----------------------------------------------------------------------------

class OutStream(object):
    """A file like object that publishes the stream to a 0MQ PUB socket."""

    # The time interval between automatic flushes, in seconds.
    flush_interval = 0.05
    topic=None

    def __init__(self, session, pub_socket, name):
        self.encoding = 'UTF-8'
        self.session = session
        self.pub_socket = pub_socket
        self.name = name
        self.parent_header = {}
        self._new_buffer()
        self._masterpid = os.getpid()

        self._buffer_thread_lock = Lock() # never touch this in child process

        context = zmq.Context()
        self._addr = 'tcp://127.0.0.1'
        self._pull_socket = context.socket(zmq.PULL)
        self._pull_port = self._pull_socket.bind_to_random_port(self._addr)
        self._debug_print('pull:: %s:%d'%(self._addr,self._pull_port))
        self._push_socket =  None# for child
        self._thispid = os.getpid() # for checking newly forked processed
        self._poll_thread = Thread(target=self._poller)
        self._poll_thread.start()

    def _init_push_socket(self):
        pid = os.getpid()
        #self._debug_print('--%d, %d--'%(pid,self._thispid))
        if pid!=self._thispid: # newly forked
            context = zmq.Context()
            self._push_socket = context.socket(zmq.PUSH)
            self._debug_print('%s:%d'%(self._addr,self._pull_port))
            self._push_socket.connect('%s:%d'%(self._addr,self._pull_port))
            self._debug_print('push init')
            self._new_buffer()
            self._thispid = pid

    def _is_master(self):
        return os.getpid()==self._masterpid

    def _poller(self):
        # only exists on master thread
        i = 0
        while True:
            self._debug_print('poll %d'%i)
            s = self._pull_socket.poll(2000) # block pull
            if s:
                m = self._pull_socket.recv()
                self._debug_print(m)
                self._debug_print('***********************')
                self.write(m)
                self.flush()
            i+=1

            #i+=1
            


    def set_parent(self, parent):
        self.parent_header = extract_header(parent)

    def close(self):
        self.pub_socket = None

    def flush(self):
        #io.rprint('>>>flushing output buffer: %s<<<' % self.name)  # dbg
        if self.pub_socket is None:
            raise ValueError(u'I/O operation on closed file')
        else:
            self._init_push_socket()
            if self._is_master():
                data = u''

                with self._buffer_thread_lock:
                    data = self._buffer.getvalue()
                    self._buffer.close()
                    self._new_buffer()
                self._debug_print('master flush %s'%data)
                if data: # master process
                    content = {u'name':self.name, u'data':data}
                    msg = self.session.send(self.pub_socket, u'stream',
                                            content=content,
                                            parent=self.parent_header,
                                            ident=self.topic)
                    if hasattr(self.pub_socket, 'flush'):
                        # socket itself has flush (presumably ZMQStream)
                        self.pub_socket.flush()

            else: # child process
                data = self._buffer.getvalue()
                self._buffer.close()
                self._new_buffer()
                if data:
                    self._debug_print('child flush %s'%data)
                    self._push_socket.send_string(data)
                    if hasattr(self._push_socket, 'flush'):
                        self._push_socket.flush()


    def isatty(self):
        return False

    def __next__(self):
        raise IOError('Read not supported on a write only stream.')

    if not py3compat.PY3:
        next = __next__

    def read(self, size=-1):
        raise IOError('Read not supported on a write only stream.')

    def readline(self, size=-1):
        raise IOError('Read not supported on a write only stream.')

    def _debug_print(self, s):
        sys.__stdout__.write('%d: %d: %s\n'%(os.getpid(),current_thread().ident,s))
        sys.__stdout__.flush()


    def write(self, string):
        if self.pub_socket is None:
            raise ValueError('I/O operation on closed file')
        else:
            self._debug_print('write %s'%string)
            self._init_push_socket()
            # Make sure that we're handling unicode
            if not isinstance(string, unicode):
                string = string.decode(self.encoding, 'replace')
            if self._is_master():
                with self._buffer_thread_lock:
                    self._buffer.write(string)
            else:
                self._buffer.write(string)

            current_time = time.time()
            if self._start <= 0:
                self._start = current_time
            elif current_time - self._start > self.flush_interval:
                self.flush()

    def writelines(self, sequence):
        if self.pub_socket is None:
            raise ValueError('I/O operation on closed file')
        else:
            for string in sequence:
                self.write(string)

    def _new_buffer(self):
        self._buffer = StringIO()
        self._start = -1
