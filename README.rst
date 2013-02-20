`Code <http://github.com/jeeveeyes/pyev-test/>`_

About
=====

This repository contains a few programs that supplement the official pyev ( http://code.google.com/p/pyev/ ) samples.

At the moment, a multiprocess TCP echo server and client are included.

multi_proc_server.py
--------------------

* Program to illustrate multiprocess TCP echo server using Pyev module.

 * Supports multiprocess model where master process listens/accepts connections
   and worker processes handles all established client connections.

 * Master process distributes connected sockets to existing workers in a round robin fashion.

 * Socket handle/FD reduction and rebuilding is done across master to worker boundaries.

 * Depends on multiprocessing module for process launch and IPC Queue.

 * Depends on Pyev module ( http://code.google.com/p/pyev/ ) for event management.
 
 * Code initially based on Pyev's sample echo server.


multi_proc_client.py
--------------------

* Program to illustrate multiprocess TCP echo client using Pyev module.

 * Supports multiprocess model where master process launches workers and worker processes
       each try to establish connections to given server independently. 

 * Each client socket does a bind on a explicitly provided pair of (IP,Port) to avoid exhausting ephemeral ports on the client side.

 * Depends on multiprocessing module for process launch and IPC Queue.

 * Depends on Pyev module ( http://code.google.com/p/pyev/ ) for event management.
 
 * Code initially based on Pyev's sample echo server.

  
Requirements
------------

CPython 2.6 (Windows is untested)

Pyev and libev.

License
-------

New BSD License
