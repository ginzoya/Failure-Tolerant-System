#!/usr/bin/python

# Code for a database instance.

import json
import boto.dynamodb2
import boto.sqs
import time
import argparse
import contextlib
import sys

import zmq
import kazoo.exceptions
import gen_ports
import kazooclientlast

import algorithm
import publishsubscribe

# Instance Naming
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

from dboperations import create, retrieve_id, retrieve_name, add, delete_id, delete_name
from boto.dynamodb2.table import Table
from algorithm import compare_seq_num, add_seq_num
from boto.sqs.message import Message
from counterlast import CounterLast
from boto.dynamodb2.fields import HashKey

seq_num = 0 # local sequence number
POLL_INTERVAL = 30 # seconds

# Polling loop to grab messages off SQS
def running_loop():
	try:
	    conn = boto.sqs.connect_to_region("us-west-2")
	    if conn == None:
	        sys.stderr.write("Could not connect to AWS region '{0}'\n".format("us-west-2"))
	        sys.exit(1)

	    # Assuming that the queues will have already been created elsewhere
	    q_in = conn.get_queue(args.in_queue)
	    q_out = conn.get_queue(args.out_queue)

	except Exception as e:
	    sys.stderr.write("Exception connecting to SQS\n")
	    sys.stderr.write(str(e))
	    sys.exit(1)

	global seq_num

	seq_hash = []
	last_performed_num = 0
	stored_messages = []

	print "Starting up instance: {0}".format(args.my_name)

	# Actual work gets done here
	while (1 < 2): # lol
		# grab a message off SQS_IN
		rs = q_in.get_messages(message_attributes=["action", "id", "name", "activities"])
		if (len(rs) < 1):
			time.sleep(POLL_INTERVAL) # wait before checking for messages again (in seconds)
			continue
		m = rs[0]
		q_in.delete_message(m) # remove message from queue so it's not read multiple times
		print "Received message: " + m.get_body()

		seq_num += 1 #increment
		loc_seq_num = seq_num.last_set #store value to local variable

		publishsubscribe.send_message(pub_socket, [loc_seq_num, m]) 
		# Publish to pub_socket the seq_num and the message

		# Runs the algorithm to check if the next number in hash is one above the last performed seq num
		calculated_num, next_in_seq = algorithm.compare_seq_num(seq_hash, last_performed_num)
		# This is the catch-up loop, running until the number after last performed seq num is loc_seq_num
		while (calculated_num < loc_seq_num): 
			if next_in_seq: # If the next number is indeed last_performed_num + 1
				for ops in stored_messages: # Loops through the list of messages
					if ops[0] == calculated_num: # Finds the right operation
						op_holder = ops # Holdes the operation
						op_found = True # Bool to say found operation
				if op_found:
					perform_operation(op_holder[1]) # Performs the operation
					# TODO: fix the call above so it looks something like:
					# perform_operation(action, input_id=id, input_name=name, input_activities=activities)
		 			last_performed_num = calculated_num #Increases the last operation done
					stored_messages.remove(op_holder) #Removes the operation from the list
			new_op, has_msg = publishsubscribe.receive_message(sub_sockets)
			# This assumes that the return is in the format [seq_num, message]
			if has_msg: # If there was an incoming message from the subscribe ports
				algorithm.add_seq_num(seq_hash, new_op[0]) # Adds the seq_num to the hash
				stored_messages.append(new_op) # Stores the message in the list
				# Compares the next smallest number in the hash
				calculated_num, next_in_seq = algorithm.compare_seq_num(seq_hash, last_performed_num)

		# NOTE: after it finally exits this loop, the seq_num should be equal to calculated_num
		# This means that calculated_num == loc_seq_num, so it is the current operation
		# If not, then something has gone wrong with the algorithm (Needs to be debugged)

		# Actually perform the operation on the db here, grab the response,
		# and put a message on the output queue
		message_out = perform_operation_msg(m)
		last_performed_num += 1

		# Send out the message to the output queue
		print "Sending message: " + message_out.get_body() # [debug]
		q_out.write(message_out)

# Performs action on the database based on in_msg
# Returns the message to go to SQS_OUT
def perform_operation_msg(in_msg):
	print "Performing operation: {0} on instance {1}".format(action, args.my_name) # [debug]
	parse_res = parse_sqs_msg(in_msg)
	action = parse_res[0]
	user_id = parse_res[1]
	user_name = parse_res[2]
	user_activities = parse_res[3]
	message_out = Message()

	if (action == "create"):
		response = create(user_id, user_name, user_activities)
		# Grab the entire json body and put in the message for SQS
		message_out.set_body(response[1])
		# Set the response code of the request as a message attribute
		message_out.message_attributes = {
			"response_code": {
				"data_type": "Number",
				"string_value": response[0]
			}
		}
	elif (action == "retrieve"):
		if (user_id != ""):
			response = retrieve_id(user_id)
		else:
			response = retrieve_name(user_name)
		message_out.set_body(response[1])
		message_out.message_attributes = {
			"response_code": {
				"data_type": "Number",
				"string_value": response[0]
			}
		}
	elif (action == "delete"):
		if (user_id != ""):
			response = delete_id(user_id)
		else:
			response = delete_name(user_name)

		message_out.set_body(response[1])
		message_out.message_attributes = {
			"response_code": {
				"data_type": "Number",
				"string_value": response[0]
			}
		}
	elif (action == "add_activities"):
		response = add(user_id, user_activities)

		message_out.set_body(response[1])
		message_out.message_attributes = {
			"response_code": {
				"data_type": "Number",
				"string_value": response[0]
			}
		}
	return message_out

# Performs the specified operation on the database, returning
# True for success.
def perform_operation(action, input_id="", input_name="", input_activities=""):
	print "Performing operation: {0} on instance {1}".format(action, args.my_name) # [debug]
	op_res = False

	if (action == "create"):
		response = create(input_id, input_name, input_activities)
		if (response[0] == 201): # success
			op_res = True
	elif (action == "retrieve"):
		if (input_id != ""):
			response = retrieve_id(input_id)
		else:
			response = retrieve_name(input_name)
		if (response[0] == 200): #success
			op_res = True
	elif (action == "delete"):
		if (input_id != ""):
			response = delete_id(input_id)
		else:
			response = delete_name(input_name)
		if (response[0] == 200):
			op_res = True
	elif (action == "add_activities"):
		response = add(input_id, input_activities)
		if (response[0] == 200):
			op_res = True
	return op_res

# Takes in a message from SQS_IN and returns a list of strings formatted like so:
# [action, id, name, activities]
# if one of them wasn't provided, it'll show up as ""
def parse_sqs_msg(in_msg):
	action = in_msg.message_attributes["action"]["string_value"]
	user_id = ""
	user_name = ""
	user_activities = ""

	if (action == "create"):
		user_id = in_msg.message_attributes["id"]["string_value"]
		user_name = in_msg.message_attributes["name"]["string_value"]
		user_activities = in_msg.message_attributes["activities"]["string_value"]
	elif (action == "retrieve"):
		user_id = in_msg.message_attributes["id"]["string_value"]
		user_name = in_msg.message_attributes["name"]["string_value"]
	elif (action == "delete"):
		user_id = in_msg.message_attributes["id"]["string_value"]
		user_name = in_msg.message_attributes["name"]["string_value"]
	elif (action == "add_activities"):
		user_id = in_msg.message_attributes["id"]["string_value"]
		user_activities = in_msg.message_attributes["activities"]["string_value"]
	return [action, user_id, user_name, user_activites]

#TODO: If anyone has any ideas for defaults/better descriptions go for it
def build_parser():
    ''' Define parser for command-line arguments '''
    parser = argparse.ArgumentParser(description="Web server demonstrating final project technologies")
    parser.add_argument("zk_string", help="ZooKeeper host string (name:port or IP:port, with port defaulting to 2181)")
    parser.add_argument("in_queue", help="Name of input queue")
    parser.add_argument("out_queue", help="Name of output queue")
    parser.add_argument("write_capacity", type=int, help="write capacity for this instance")
    parser.add_argument("read_capacity", type=int, help="read capacity for this instance")
    parser.add_argument("my_name", help="name of this particular instance")
    parser.add_argument("db_names", help="list of instance names for the databases (comma-separated)")
    parser.add_argument("proxy_list", help="List of instances to proxy, if any (comma-separated)")
    parser.add_argument("base_port", type=int, help="Base port for publish/subscribe")
    return parser

def get_ports():
	''' Return the publish port and the list of subscribe ports '''
	if args.db_names != '':
		db_names = args.db_names.split(',')
	else:
		db_names = []

	#has_proxies = False

	if args.proxy_list != '':
		proxies = args.proxy_list.split(',')
		#has_proxies = True
	else:
		proxies = []

	return gen_ports.gen_ports(args.base_port, db_names, proxies, args.my_name)

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
	print "instance {0} binding on {1}".format(args.my_name, pub_port)
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
		print "instance {0} connecting to {1} on {2}".format(args.my_name, sub_to_name, sub_port)
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
    
def create_table():
	try:
		users = Table.create(args.my_name, 
			schema=[
				HashKey('id'),
			], 
			throughput={
				'read': args.read_capacity,
				'write': args.write_capacity,
			},
			connection=boto.dynamodb2.connect_to_region('us-west-2')
			)
		print "Table created!"
	except:
		print "already exists!"
	finally:
		return

def main():
	'''
		Initializes an instance of a dynamoDB.
	'''
	global args
	global seq_num

	parser = build_parser() #build parser
	args = parser.parse_args()
	if (args.proxy_list == "NONE"):
		args.proxy_list = ""
	print args.proxy_list

	create_table()

	# Open connection to ZooKeeper and context for zmq
	with kzcl(kazooclientlast.KazooClientLast(hosts=args.zk_string)) as kz, \
		zmqcm(zmq.Context.instance()) as zmq_context:

		# Set up publish and subscribe sockets
		setup_pub_sub(zmq_context, SUB_TO_NAME)

		# Initialize sequence numbering by ZooKeeper
		try:
			kz.create(path=SEQUENCE_OBJECT, value="0", makepath=True)
		except kazoo.exceptions.NodeExistsError as nee:
			kz.set(SEQUENCE_OBJECT, "0") # Another instance has already created the node
										 # or it is left over from prior runs

		# Wait for all DBs to be ready
		barrier_path = APP_DIR+BARRIER_NAME
		kz.ensure_path(barrier_path)
		b = kz.create(barrier_path + '/' + args.my_name, ephemeral=True)

		if args.db_names != '':
			db_names = args.db_names.split(',')
			num_dbs = len(db_names)
		else:
			db_names = []
			num_dbs = len(db_names)

		while len(kz.get_children(barrier_path)) < num_dbs:
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
		
		running_loop()

if __name__ == "__main__":
    main()
