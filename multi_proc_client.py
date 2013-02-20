#!/bin/python
"""
Program to illustrate multiprocess TCP echo client using Pyev module.

  - Supports multiprocess model where master process launches workers and worker processes
       each try to establish connections to given server independently. 

  - Each client socket does a bind on a explicitly provided pair of (IP,Port) to avoid exhausting ephemeral ports on the client side.

  - Depends on multiprocessing module for process launch and IPC Queue.

  - Depends on Pyev module ( http://code.google.com/p/pyev/ ) for event management.
 
  - Code initially based on Pyev's sample echo server.

"""

#This file is placed under the "New BSD License" just like Pyev itself.

#Copyright (c) 2013, Sriraam Vijayaraghavan
#Some rights reserved.

#    Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

#    Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

#    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

#    Neither the name of the <ORGANIZATION> nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

#    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import time
import multiprocessing
from multiprocessing.reduction import reduce_handle, rebuild_handle
import Queue
import os 
import socket
import signal
import weakref
import errno
import logging
import sys
import pyev

try:
    import resource
    resource.setrlimit(resource.RLIMIT_NOFILE, (5000000, -1))
except ValueError:
    pass 

logging.basicConfig(level=logging.INFO)

STOPSIGNALS = (signal.SIGINT, signal.SIGTERM)
NONBLOCKING = (errno.EAGAIN, errno.EWOULDBLOCK)

class Connection(object):
    """ Handles data IO for a single TCP connection """

    def __init__(self, parent_worker, cnxn_id, sock, client_address, server_address):
        self.parent_worker = parent_worker
        self.cnxn_id = cnxn_id
        self.sock = sock
        self.server_address = server_address
        self.client_address = client_address
        self.loop = self.parent_worker.loop

        self.buf = ""
        self.req = "GET / HTTP/1.1\r\nUser-Agent: multi_proc_client/0.1.0\r\nHost: %s\r\n\r\n"%(self.client_address[0],)

        logging.debug("[%s:%d] Connection object instantiated"%(self.parent_worker.name,self.cnxn_id))

    def do_connect(self):

        self.watcher = pyev.Io(self.sock.fileno(), pyev.EV_WRITE, self.loop, self.io_cb)
        self.watcher.start()

        try:

            logging.debug("[%s:%d] Connecting to [%s] from [%s] ..."%(self.parent_worker.name,self.cnxn_id,str(self.server_address),str(self.client_address)))

            self.sock.connect_ex(self.server_address)    

        except Exception as err:
            if err.args[0] not in NONBLOCKING:
                logging.error("[%s:%d] Error connecting to [%s]"%(self.parent_worker.name,self.cnxn_id,str(self.server_address)),exc_info=True)

    def reset(self, events):
        self.watcher.stop()
        self.watcher.set(self.sock, events)
        self.watcher.start()

    def handle_error(self, msg, level=logging.ERROR, exc_info=True):
        logging.log(level, "{0}: {1} --> closing".format(self, msg),
                    exc_info=exc_info)
        self.close()

    def handle_read(self):
        buf = ""
        #logging.debug("[%s:%d] handle_read called "%(self.parent_worker.name,self.cnxn_id))
        try:
            buf = self.sock.recv(1024)
        #except socket.error as err:
        except Exception as err:
            logging.debug("[%s:%d] handle_read socket error : %s"%(self.parent_worker.name,self.cnxn_id,str(err)))
            if err.args[0] not in NONBLOCKING:
                self.handle_error("error reading from {0}".format(self.sock))
                return
            #else:
            #    return
        if len(buf):
            #logging.debug("[%s:%d] Received data: %s"%(self.parent_worker.name,self.cnxn_id,buf))
            self.buf += buf
            self.reset(pyev.EV_READ | pyev.EV_WRITE)
        else:
            self.handle_error("Graceful connection closed by peer", logging.DEBUG, False)

    def handle_write(self):
        #logging.debug("[%s:%d] handle_write called "%(self.parent_worker.name,self.cnxn_id))
        try:
            sent = self.sock.send(self.req)
        except socket.error as err:
            if err.args[0] not in NONBLOCKING:
                self.handle_error("error writing to {0}".format(self.sock))
        else :
            self.reset(pyev.EV_READ)
            #self.buf = self.buf[sent:]
            #if not self.buf:
            #    self.reset(pyev.EV_READ)

    def io_cb(self, watcher, revents):
        #logging.debug("[%s:%d] io_cb called "%(self.parent_worker.name,self.cnxn_id))
        if revents & pyev.EV_READ:
            self.handle_read()
        elif revents & pyev.EV_WRITE:
            self.handle_write()
        else:
            logging.debug("[%s:%d] io_cb called with unknown event %s"%(self.parent_worker.name,self.cnxn_id,str(revents)))

    def close(self):
        self.sock.close()
        self.watcher.stop()
        self.watcher = None
        logging.debug("{0}: closed".format(self))


class ClientWorker(multiprocessing.Process):

    def __init__(self,
                 name,
                 in_q,
                 out_q,
                 client_ip,
                 start_client_port,
                 end_client_port,
                 num_cnxn_per_client,
                 num_cnxn_per_sec,
                 server_ip,
                 server_port):

        multiprocessing.Process.__init__(self,group=None,name=name)

        self.in_q = in_q
        self.out_q = out_q
        self.client_ip = client_ip
        self.start_client_port = start_client_port
        self.end_client_port = end_client_port
        self.num_cnxn_per_client = num_cnxn_per_client
        self.num_cnxn_per_sec = num_cnxn_per_sec
        self.server_ip = server_ip
        self.server_port = server_port
        self.server_address = (self.server_ip,self.server_port)

        self.loop = pyev.Loop(flags=pyev.EVBACKEND_EPOLL)

        self.watchers = []
        self.cur_cnxn_count = 0
        self.next_client_port = self.start_client_port

        #self.watchers.extend(pyev.Signal(sig, self.loop, self.signal_cb)
        #                 for sig in STOPSIGNALS])

        # Obtain the Queue object's underlying FD for use in the event loop
        self.in_q_fd = self.in_q._reader.fileno()

        self.watchers.append(pyev.Io(self.in_q_fd, 
                                     pyev.EV_READ, 
                                     self.loop,
                                     self.in_q_cb))


        self.cnxns = {}
        logging.debug("ClientWorker ["+self.name+"] instantiated.") 

    def run(self):

        for watcher in self.watchers:
            watcher.start()

        # initialise and start a repeating timer
        self.timer = self.loop.timer(0, 1, self.timer_cb, 0)
        self.timer.start()

        logging.info("ClientWorker[{0}:{1}]: Running...".format(os.getpid(),self.name))
        self.loop.start()
        logging.info("ClientWorker[{0}:{1}]: Exited event loop!".format(os.getpid(),self.name))


    def stop(self):

        self.timer.stop()

        while self.watchers:
            self.watchers.pop().stop()

        self.loop.stop(pyev.EVBREAK_ALL)

        self.out_q.put("quitdone")

        logging.info("ClientWorker[{0}:{1}]: Stopped!".format(os.getpid(),self.name))

        sys.exit(0)

    def reset_q_watcher(self, events):

        self.watchers[0].stop()
        self.watchers[0].set(self.in_q_fd, events)
        self.watchers[0].start()

    def signal_cb(self, watcher, revents):

        self.stop()

    def in_q_cb(self, watcher, revents):
        try:
            val = self.in_q.get()
            #val = self.in_q.get(True,interval)
            logging.debug("ClientWorker ["+self.name+"] received inQ event ") 
            if type(val) == type((1,)):
                pass
            elif type(val) == type("") and val == "quit":
                logging.info("ClientWorker[{0}:{1}]: Received quit message!".format(os.getpid(),self.name))
                self.stop()

        except Queue.Empty:
            # Timed-out, carry on
            pass
        
    def timer_cb(self,watcher, revents):

        print("timer.loop.iteration: {0}".format(watcher.loop.iteration))
        print("timer.loop.now(): {0}".format(watcher.loop.now()))
        
        for cnxn_index in range(self.num_cnxn_per_sec):

            if self.cur_cnxn_count >= self.num_cnxn_per_client:
                logging.debug("ClientWorker ["+self.name+"] established ["+str(self.num_cnxn_per_client)+"] connections") 
                self.timer.stop()
                break
            
            next_cnxn_id = self.cur_cnxn_count + 1

            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            client_sock.setblocking(0)
            client_sock.settimeout(0.5)

            # Attempt bind on successive ports until we successfully bind or reach port upper limit
            while 1:

                addr_to_try = (self.client_ip,self.next_client_port)

                try:
                    # Lets try the next client IP + local port 
                    client_sock.bind(addr_to_try)
                except socket.error as err:
                    logging.error("ClientWorker ["+self.name+"] could not bind to port ["+str(self.next_client_port)+"]",True)
                    
                    self.next_client_port += 1
                    
                    # We can try only as far as the upper bound
                    if self.next_client_port > self.end_client_port:
                        logging.error("ClientWorker ["+self.name+"] Reached max ports allowed  ["+str(self.end_client_port)+"]")
                        return

                else:
                    self.next_client_port += 1
                    break

            cnxn = Connection(self, next_cnxn_id, client_sock, addr_to_try, self.server_address)
            self.cnxns[next_cnxn_id] = cnxn
            cnxn.do_connect()
            self.cur_cnxn_count += 1            


class ClientMaster(object):

    def __init__(self, 
            start_client_ip="127.0.0.1",
            start_client_port=10000,
            end_client_port=60000,
            num_cnxn_per_client=10,
            num_cnxn_per_sec=1,
            start_server_ip="127.0.0.1",
            start_server_port=5000,
            num_client_workers=1):

        self.worker_procs = []
        self.worker_queues = []

        client_ip = start_client_ip
        server_ip = start_server_ip

        for i in range(num_client_workers):

            # Create a pair of (inQ,outQ) for IPC with the worker
            worker_in_q = multiprocessing.Queue()
            worker_out_q = multiprocessing.Queue()

            self.worker_queues.append((worker_in_q,worker_out_q))

            # Create the worker process object
            worker_proc = ClientWorker("CW."+str(i+1), 
                                       worker_in_q,
                                       worker_out_q,
                                       client_ip,
                                       start_client_port,
                                       end_client_port,
                                       num_cnxn_per_client,
                                       num_cnxn_per_sec,
                                       server_ip,
                                       start_server_port 
                                       )

            worker_proc.daemon = True
            self.worker_procs.append(worker_proc)
        
            # Start the worker process
            worker_proc.start()
    
        # By now the client workers have been spawned

        # Setup the default Pyev loop in the master 
        self.loop = pyev.default_loop(flags=pyev.EVBACKEND_EPOLL)

        # Prepare signal and out Q watchers
        self.sig_watchers = [pyev.Signal(sig, self.loop, self.signal_cb)
                              for sig in STOPSIGNALS]

        self.q_watchers = [pyev.Io(fd=worker.out_q._reader.fileno(), 
                                  events=pyev.EV_READ,
                                  loop=self.loop, 
                                  callback=self.out_q_cb,
                                  data=worker)
                            for worker in self.worker_procs]


    def start(self):

        for watcher in self.sig_watchers:
            watcher.start()

        for watcher in self.q_watchers:
            watcher.start()

        logging.info("ClientMaster[{0}]: Started...".format(os.getpid()))

        self.loop.start()

    def stop(self):

        logging.info("ClientMaster[{0}]: Stop requested.".format(os.getpid()))

        for worker in self.worker_procs:
            worker.in_q.put("quit")

        while self.sig_watchers:
            self.sig_watchers.pop().stop()

        for worker in self.worker_procs:
            worker.join()

        while self.q_watchers:
            self.q_watchers.pop().stop()

        self.loop.stop(pyev.EVBREAK_ALL)

        logging.info("ClientMaster[{0}]: Stopped!".format(os.getpid()))

    def handle_error(self, msg, level=logging.ERROR, exc_info=True):

        logging.log(level, "ClientMaster[{0}]: Error: {1}".format(os.getpid(), msg),
                    exc_info=exc_info)
        self.stop()

    def signal_cb(self, watcher, revents):

        logging.info("ClientMaster[{0}]: Signal triggered.".format(os.getpid()))
        self.stop()

    def out_q_cb(self, watcher, revents):

        try:
            val = watcher.data.out_q.get()
            #val = self.in_q.get(True,interval)
            logging.debug("ClientMaster received outQ event from [%s] data [%s]"\
                    %(watcher.data.name,str(val))) 
            if type(val) == type((1,)):
                pass
            elif type(val) == type("") and val == "quitdone":
                logging.debug("ClientWorker [%s] has quit"\
                    %(watcher.data.name,)) 

        except Queue.Empty:
            # Timed-out, carry on
            pass


if __name__ == "__main__":

    client_master = ClientMaster(
                       start_client_ip="127.0.0.1",
                       start_client_port=10000,
                       end_client_port=60000,
                       num_cnxn_per_client=20000,
                       num_cnxn_per_sec=1000,
                       start_server_ip="127.0.0.1",
                       start_server_port=5000,
                       num_client_workers=1)

    client_master.start()

