#!/usr/bin/python

# Code for a database instance.

import json
import boto.dynamodb2
import boto.sqs
import time

from dboperations import create, retrieve_id, retrieve_name, add, delete_id, delete_name
from boto.dynamodb2.table import Table
from algorithm import compare_seq_num, add_seq_num
from boto.sqs.message import Message

# TODO: change the instance name to the one specified in the startup script
INSTANCE_NAME = "DB1"

AWS_REGION = "us-west-2"
IN_QUEUE = "SQS_IN"
OUT_QUEUE = "SQS_OUT"

seq_num = 0 # local sequence number
POLL_INTERVAL = 30 # seconds

# Polling loop to grab messages off SQS
def main_loop():
	try:
	    conn = boto.sqs.connect_to_region(AWS_REGION)
	    if conn == None:
	        sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
	        sys.exit(1)

	    # Assuming that the queues will have already been created elsewhere
	    q_in = conn.get_queue(IN_QUEUE)
	    q_out = conn.get_queue(OUT_QUEUE)

	except Exception as e:
	    sys.stderr.write("Exception connecting to SQS\n")
	    sys.stderr.write(str(e))
	    sys.exit(1)

	print "Starting up instance: {0}".format(INSTANCE_NAME)

	# Actual work gets done here
	while (1 < 2): # lol
		# grab a message off SQS_IN
		rs = q_in.get_messages()
		if (len(rs) < 1):
			time.sleep(POLL_INTERVAL) # wait before checking for messages again (in seconds)
			continue
		m = rs[0]
		q_in.delete_message(m) # remove message from queue so it's not read multiple times
		operation = m.get_body()
		print "Received message: " + operation
		# TODO: ZK calls here

		# TODO: check algorithm to see if we can run the operation.
		# We're gonna need a shared heap between all instances here.

		# compare_seq_num(heap, seq_num)

		# TODO: actually perform the operation on the db

		# put a response on the output queue
		message_out = Message()
		# TODO: replace this with an actual response
		message_out.set_body("the thing worked")
		print "Sending message: " + message_out.get_body()
		q_out.write(message_out)
	return

main_loop()