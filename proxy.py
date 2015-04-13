#!/usr/bin/env python

# Implement a proxy that reorders/resends/delays messages

import zmq
import sys
import time
import random
import argparse
import threading

# Local files
import gen_ports

BASE_PORT = 7778

DEFAULT_DELAY = 0
DEFAULT_REPEAT = 0.0
DEFAULT_INSTANCE = "DB1"

cv = threading.Condition()
messages = []

# One thread gathers packets from the outside world
def gather():
	global messages
	global cv
	global args
	context = zmq.Context.instance()
	req_socket = context.socket(zmq.SUB)
	req_socket.setsockopt(zmq.SUBSCRIBE,"")
	req_socket.connect("tcp://localhost:{0}".format(subport))

	while True:
		# without this recv its necessary for the thread to 
		# go to sleep to allow the forward thread to process items
		msg = req_socket.recv()
		cv.acquire()
		#print sys.argv[0],"got",msg
		messages.append(msg)
		count = 0
		start = time.time()
		while count < 4 and time.time() - start < 10:
			try:
				msg = req_socket.recv(zmq.NOBLOCK)
			except zmq.ZMQError as zerr:
				if zerr.errno == zmq.EAGAIN:
					pass
				else:
					raise zerr
			else:
				#print sys.argv[0],"got",msg
				messages.append(msg)
				count = count + 1
		cv.notify()
		cv.release()
	# without the blocking .recv above its necessary to sleep
	# time.sleep(1)

# Randomly duplicate the messages in place
def dup(messages, prob):
	dups = []
	for msg in messages:
		if random.random() < prob:
			dups.append(msg)
	messages.extend(dups)

# Another thread then sends them and waits for a response
def forward():
	global messages
	global cv
	global args
	context = zmq.Context.instance()
	rep_socket = context.socket(zmq.PUB)
	rep_socket.bind("tcp://*:{0}".format(pubport))

	thread = threading.Thread(target=gather)
	thread.start()
	while True:
		#print "waiting on cv"
		cv.acquire()
		#print "acquired cv"
		try:
			while len(messages) == 0:
				#print "no messages"
				cv.wait()

			dup(messages, args.repeat)
			# reorder the packets before sending
			random.shuffle(messages)
			print "len", len(messages)
			while len(messages) > 0:
				msg = messages.pop()
				if args.delay > 0:
					time.sleep(random.randrange(0,args.delay))
				#print sys.argv[0],"relaying",msg
				rep_socket.send(msg)
				# rep = rep_socket.recv()
				# fwd_socket.send(rep)
		except KeyboardInterrupt as kbe:
			print "KBI in forward()"
			raise kbe
		except Exception:
			pass
		finally:
			cv.release()

def main():
	global args
	global subport
	global pubport

	parser = argparse.ArgumentParser(
		description="0mq publish/subscribe bad proxy: delays, reorders, resends messages"
	)
	parser.add_argument(
		'base_port',
		type=int,
		nargs='?',
		default=BASE_PORT,
		help="Base port (default {0})".format(BASE_PORT)
		)
	parser.add_argument(
		'instances',
		nargs='?',
		default=DEFAULT_INSTANCE,
		help="List of instance names (comma-separated string)"
		)
	parser.add_argument(
		'proxied_instances',
		nargs='?',
		default=DEFAULT_INSTANCE,
		help="Names of proxied instances (comma-separated string)"
		)
	parser.add_argument(
		'owner_instance',
		nargs='?',
		default=DEFAULT_INSTANCE,
		help="Instance for which this will proxy"
		)
	parser.add_argument(
		'--seed',
		type=int,
		default=None,
        help="Random seed (integer)"
		)
	parser.add_argument(
		'--delay',
		type=int,
		default=DEFAULT_DELAY,
        help="Delay for all messages (int, s)"
		)
	parser.add_argument(
		'--repeat',
		type=float,
		default=DEFAULT_REPEAT,
		help="Probability of a msg repeating (float, [0, 1.0))"
		)
	args = parser.parse_args()
	if args.proxied_instances == '':
		sys.stderr.write("proxied_instances must name at least one instance\n")
		sys.exit(1)
	else:
		proxies = args.proxied_instances.split(',')
	pubport, subport = gen_ports.gen_ports(
		args.base_port,
		args.instances.split(','),
		proxies,
		args.owner_instance,
		True
	)
	# start up the random number generator in a predictable way
	random.seed(args.seed)
	fwd_thread = threading.Thread(target=forward)
	fwd_thread.start()

if __name__ == "__main__":
	main()

