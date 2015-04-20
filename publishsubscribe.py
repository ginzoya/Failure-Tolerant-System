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
	print message #DEBUG
	pub_socket.send_json(message)

def receive_message(sub_sockets):
	if len(sub_sockets) == 0:
		noSubs = ["No Subscriptions", False]
		return noSubs
	else:
		for sub_socket in sub_sockets:
			num_events = sub_socket.poll(timeout=5)
			if (num_events > 0):
				try:
					message_content = sub_socket.recv_json(zmq.NOBLOCK)
					success = [message_content, True]
					return success
				except zmq.ZMQError as zerr:
					if zerr.errno == zmq.EAGAIN:
						error = ["Empty Message", False]
						return error
						# Not an empty message, but worse
						raise zerr
	noMsg = ["No Messages", False]
	return noMsg
