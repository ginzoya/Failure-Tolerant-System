#!/usr/bin/env python
'''
    The purpose of this script is to take in an instance's sequence
    and operation and publish it to other instances.
'''

import zmq
'''
    Takes gathers the sequence number and operation of an instance
    and returns a tuple. 
    NOTE: Sequence numbers not used, instead receiver will scan
    the list of sockets (5 seconds per entry) for new events.
'''

def send_message(pub_socket, message):    
	print "Instance: " + instance + " publishing: " + message
	pub_socket.send_json(message)

def receive_message(sub_sockets):
	if len(sub_sockets) == 0:
		response.status = 404
		return "No Subscriptions", False

	else:
		for sub_socket in sub_sockets:
			num_events = sub_socket.poll(timeout=5)
			if (num_events > 0):
				try:
					message_content = sub_socket.recv_json(zmq.NOBLOCK)
					# TODO: Format of message. Thinking that it should be:
						# [Seq_num, Message]
					return message_content, True

				except zmq.ZMQError as zerr:
					if zerr.errno == zmq.EAGAIN:
						return "Empty Message", False
						# Not an empty message, but worse
						raise zerr
