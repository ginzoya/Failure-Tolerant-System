#!/usr/bin/env python

'''
  Boilerplate for final project, CMPT 474.

  This file demonstrates several technologies that you may
  use in your final project:

  1. Building a Web server using bottle.
  2. Reading positional command-line arguments using argparse.
  3. Establishing a connection to ZooKeeper via the Kazoo client.
  4. Using ZooKeeper to rendezvous several instances to a common
     start point.
  5. Maintaining a request id across system starts.

  There are several AWS services that you will also have to
  call in the project. They are not demonstrated here because
  you are assumed to already know them from earlier exercises
  and assignments.

  IN YOUR ACTUAL PROJECT, THESE TECHNOLOGIES WILL NOT ALL BE
  USED IN THE SAME PROCESS. THIS FILE DEMONSTRATES CALLING THE
  VARIOUS LIBRARIES, NOT WHERE THEY WILL BE USED IN THE ACTUAL
  PROJECT ARCHITECTURE.

'''

# Standard library packages
import sys
import json
import time
import signal
import os.path
import argparse
import contextlib

# Installed packages
'''
  pyzmq tutorial: http://learning-0mq-with-pyzmq.readthedocs.org/en/latest/pyzmq/pyzmq.html
  pyzmq reference: http://zeromq.github.io/pyzmq/api/
  zmq reference: http://api.zeromq.org/
'''
import zmq
'''
  kazoo tutorial and reference: https://kazoo.readthedocs.org/en/latest/
'''
import kazoo.client
'''
  bottle tutorial and reference: http://bottlepy.org/docs/dev/index.html
'''
from bottle import route, run, request, response, abort, default_app

# Local modules
import table
import retrieve
import gen_ports

REQ_ID_FILE = "reqid.txt"

WEB_PORT = 8080

# Instance naming
BASE_INSTANCE_NAME = "DB"

# Names for ZooKeeper hierarchy
APP_DIR = "/" + BASE_INSTANCE_NAME
PUB_PORT = "/Pub"
SUB_PORTS = "/Sub"
SEQUENCE_OBJECT = APP_DIR + "/SeqNum"
DEFAULT_NAME = BASE_INSTANCE_NAME + "1"
BARRIER_NAME = "/Ready"

# Publish and subscribe constants
SUB_TO_NAME = 'localhost' # By default, we subscribe to our own publications
BASE_PORT = 7777


def build_parser():
    ''' Define parser for command-line arguments '''
    parser = argparse.ArgumentParser(description="Web server demonstrating final project technologies")
    parser.add_argument("zkhost", help="ZooKeeper host string (name:port or IP:port, with port defaulting to 2181)")
    parser.add_argument("web_port", type=int, help="Web server port number", nargs='?', default=WEB_PORT)
    parser.add_argument("name", help="Name of this instance", nargs='?', default=DEFAULT_NAME)
    parser.add_argument("number_dbs", type=int, help="Number of database instances", nargs='?', default=1)
    parser.add_argument("base_port", type=int, help="Base port for publish/subscribe", nargs='?', default=BASE_PORT)
    parser.add_argument("sub_to_name", help="Name of system to subscribe to", nargs='?', default=SUB_TO_NAME)
    parser.add_argument("proxy_list", help="List of instances to proxy, if any (comma-separated)", nargs='?', default="")
    return parser

def get_ports():
    ''' Return the publish port and the list of subscribe ports '''
    db_names = [BASE_INSTANCE_NAME+str(i) for i in range(1, 1+args.number_dbs)]
    print db_names, args.proxy_list.split(',')
    if args.proxy_list != '':
        proxies = args.proxy_list.split(',')
    else:
        proxies = []
    return gen_ports.gen_ports(args.base_port, db_names, proxies, args.name)


def setup_pub_sub(zmq_context, sub_to_name):
    ''' Set up the publish and subscribe connections '''
    global pub_socket
    global sub_sockets

    pub_port, sub_ports = get_ports()

    '''
      Open a publish socket. Use a 'bind' call.
    '''
    pub_socket = zmq_context.socket(zmq.PUB)
    '''
      The bind call does not take a DNS name, just a port.
    '''
    print "instance {0} binding on {1}".format(args.name, pub_port)
    pub_socket.bind("tcp://*:{0}".format(pub_port))

    sub_sockets = []
    for sub_port in sub_ports:
        '''
          Open a subscribe socket. Use a 'connect' call.
        '''
        sub_socket = zmq_context.socket(zmq.SUB)
        '''
          You always have to specify a SUBSCRIBE option, even
          in the case (such as this) where you are subscribing to
          every possible message (indicated by "").
        '''
        sub_socket.setsockopt(zmq.SUBSCRIBE, "")
        '''
          The connect call requires the DNS name of the system being
          subscribed to.
        '''
        print "instance {0} connecting to {1} on {2}".format(args.name, sub_to_name, sub_port)
        sub_socket.connect("tcp://{0}:{1}".format(sub_to_name, sub_port))
        sub_sockets.append(sub_socket)

@contextlib.contextmanager
def zmqcm(zmq_context):
    '''
      This function wraps a context manager around the zmq context,
      allowing the client to be used in a 'with' statement. Simply
      use the function without change.
    '''
    try:
        yield zmq_context
    finally:
        print "Closing sockets"
        # The "0" argument destroys all pending messages
        # immediately without waiting for them to be delivered
        zmq_context.destroy(0)

@contextlib.contextmanager
def kzcl(kz):
    '''
      This function wraps a context manager around the kazoo client,
      allowing the client to be used in a 'with' statement.  Simply use
      the function without change.
    '''
    kz.start()
    try:
        yield kz
    finally:
        print "Closing ZooKeeper connection"
        kz.stop()
        kz.close()

def main():
    '''
       Main routine. Initialize everything, wait
       for all the other instances to complete their
       initialization, then begin responding to requests.

       The following list of `global` statements are required
       by one of the more awkward parts of Python syntax: If you
       assign to a global variable anywhere inside a function, it
       is safest to declare that variable `global` at the
       top of the function.

       Strictly speaking, you don't have to do this in every 
       case, but it's simplest to just observe this rule and
       avoid mystifying bugs in the cases where it is required.

       If you're only *reading* a global variable inside a
       function, you don't need to declare it. For example,
       retrieve_route() doesn't declare `args` because it
       only reads that global variable.
    '''
    global args
    global table
    global seq_num
    global req_file
    global request_count

    # Get the command-line arguments
    parser = build_parser()
    args = parser.parse_args()

    # Set up the table
    table = table.open_table()

    # Initialize request id from durable storage
    if not os.path.isfile(REQ_ID_FILE):
        with open(REQ_ID_FILE, 'w', 0) as req_file:
            req_file.write("0\n")

    try:
        req_file = open(REQ_ID_FILE, 'r+', 0)
        request_count = int(req_file.readline())
    except IOError as exc:
        sys.stderr.write(
            "Exception reading request id file '{0}'\n".format(REQ_ID_FILE))
        sys.stderr.write(exc)
        sys.exit(1)

    # Open connection to ZooKeeper and context for zmq
    with kzcl(kazoo.client.KazooClient(hosts=args.zkhost)) as kz, \
        zmqcm(zmq.Context.instance()) as zmq_context:

        # Set up publish and subscribe sockets
        setup_pub_sub(zmq_context, args.sub_to_name)

        # Initialize sequence numbering by ZooKeeper
        if not kz.exists(SEQUENCE_OBJECT):
            kz.create(SEQUENCE_OBJECT, "0")
        else:
            kz.set(SEQUENCE_OBJECT, "0")

        # Wait for all DBs to be ready
        barrier_path = APP_DIR+BARRIER_NAME
        kz.ensure_path(barrier_path)
        b = kz.create(barrier_path + '/' + args.name, ephemeral=True)
        while len(kz.get_children(barrier_path)) < args.number_dbs:
            time.sleep(1)
        print "Past rendezvous"

        # Now the instances can start responding to requests

        '''
          Create the sequence counter.
          This creates client-side links to a common structure
          on the server side, so it has to be done *after* the
          rendezvous.
        '''
        seq_num = kz.Counter(SEQUENCE_OBJECT)

        '''
          Start the Web server.
          Because the server is running within the with block for the 
          ZooKeeper client, the connection to ZooKeeper will remain
          open for as long as the server runs. 

          To stop the server cleanly (ensuring cleanup of ZooKeeper and zmq),
          either use ^C (for a server running in the foreground) or
          `kill -2 <pid>` from the command line (for a server running
          in the background).  If you kill the server using any
          other signal than 2 (keyboard interrupt), ZooKeeper and zmq
          will not be cleaned up.  The other signals can be caught (and probably
          should in a production system) but the code to do so would complicate
          this demonstration.
        '''
        app = default_app()
        run(app, host="localhost", port=args.web_port)
    
@route('/retrieve')
def retrieve_route():
    ''' Implement retrieve call '''
    global request_count

    id = int(request.query.id)

    print ("instance {0} retrieving id {1}".format(args.name, id))

    result = retrieve.do_retrieve(table, args.name, id, request_count)

    request_count += 1
    req_file.seek(0)
    req_file.write(str(request_count))
    req_file.flush()
    os.fsync(req_file.fileno())

    if result:
        return {
            "data": {
                "type": "person",
                "id": str(id),
                "name": result['name'],
                "activities": result['activities']
                }
            }
    else:
        response.status = 404
        return {
            "errors": [{
                "not_found": {
                    "id": str(id)
                    }
                }]
            }

'''
  The following two calls demonstrate using publish and subscribe.

  Your actual project will not implement these REST calls
  and will use publish/subscribe to communicate between database
  instances
'''

'''
  To send a message:
  /send?msg=<message>   <message> cannot have any spaces (HTTP restriction)
'''
@route('/send')
def send():
    msg = request.query.msg
    
    print ("Instance {0} publishing {1}".format(args.name, msg))

    pub_socket.send_string(msg)

    return {
        "data": {
            "type": "Message sent"
            }
        }

'''
  To receive a message published by /send:
  /receive
'''
@route('/receive')
def receive():
    sub_index = int(request.query.port_index)
    
    if len(sub_sockets) == 0:
        response.status = 404
        return {
            "errors": [{
                "no subscriptions": {
                    "msg": "This instance is not subscribed to any publishers"
                    }
                }]
            }
    elif sub_index >= len(sub_sockets):
        response.status = 404
        return {
            "errors": [{
                "index out of range": {
                    "msg": "The subscription index {0} is out of the " +
                        "range [0 ... {1})".format(sub_index, len(sub_sockets))
                    }
                }]
            }

    '''
      This is a non-blocking read: If there is a message already
      enqueued, the recv() call will return it. If are no messages waiting,
      the recv() call will raise a zmq.EAGAIN exception.

      There are other exceptions that might be raised during recv().
      These are serious errors and we will just re-raise them, ending the
      application. (And of course the context managers in the main() routine
      will be invoked when they are re-raised, ensuring that ZooKeeper
      and zmq shut down cleanly.)
    '''
    try:
        msg = sub_sockets[sub_index].recv(zmq.NOBLOCK)
    except zmq.ZMQError as zerr:
        if zerr.errno == zmq.EAGAIN:
            # No messages queued
            return {
                "data": {
                    "type": "empty_message",
                    }
                }
        else:
            # Something awful--just quit
            raise zerr
    else:
        return {
            "data": {
                "type": "message",
                "msg": msg
                }
            }

# Standard Python shmyntax for the main file in an application
if __name__ == "__main__":
    main()
    
