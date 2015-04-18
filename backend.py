#!/usr/bin/python

import boto.sqs
import argparse
import sys
from boto.sqs.message import Message
from bottle import Bottle, run, route, request, response, template

AWS_REGION = "us-west-2"
PORT = 8081
app = Bottle()

'''
	Build the parsed arguments, made a function incase we want to add more
'''
def build_parser():
	parser = argparse.ArgumentParser(description="Web server demonstrating final project technologies")
	parser.add_argument("out_queue", help="name of sqs output queue")
	return parser
    
def main():
	global args
	parser = build_parser()
	args = parser.parse_args()
	
	try:
		conn = boto.sqs.connect_to_region(AWS_REGION)
	except Exception as e:
		sys.stderr.write("Exception connecting to SQS\n")
		sys.stderr.write(str(e))
		sys.exit(1)
	if conn == None:
		sys.stderr.write("Could not connect to AWS region '{0}'\n".format(AWS_REGION))
		sys.exit(1)

    # Assume the queue is ready
	q_out = conn.create_queue(args.out_queue)
	
	run(app, host="localhost", port=PORT)

@app.route('/')
def app():
    # grab a message off SQS_IN
	rs = q_out.get_messages(message_attributes=["response_code"])
	if (len(rs) < 1):
		print "No messages on the queue!"
		response.status = 204 # No content
		response_body = "Queue empty\n"
		# TODO: for some reason this doesn't return "Queue empty" in the user's browser.
		# That needs to be fixed. Otherwise, this seems to be working fine
		return response_body

	m = rs[0]
	q_out.delete_message(m) # remove message from queue so it's not read multiple times

	if m == None:
	    response.status = 204 # "No content"
	    response_body = "Queue empty\n"
	    return response_body
	else:
	    response_code = m.message_attributes["response_code"]["string_value"]
<<<<<<< HEAD

	    response_body = m.get_body()
	    response.status = int(response_code)
=======

	    response_body = m.get_body()
	    response.status = int(response_code)

	    #print response_body # [debug]
	    return response_body
>>>>>>> 276fd4d4a6f184c039ebeae9c8b11f560e70edd8

	    #print response_body # [debug]
	    return response_body

# An example message would look like this:
# {
# 	"action": 
# 	{ 
# 		"data_type": "String", 
# 		"string_value": "create" 
# 	}, 
# 	"id": 
# 	{
# 		"data_type": "String", 
# 		"string_value": "13" 
# 	}, 
# 	"name": 
# 	{ 
# 		"data_type": "String", 
# 		"string_value": "John" 
# 	}, 
# 	"activities": 
# 	{ 
# 		"data_type": "String", 
# 		"string_value": "{eating,swimming}" 
# 	}
# }

if __name__ == "__main__":
    main()
