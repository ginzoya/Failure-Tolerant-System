#!/usr/bin/env python

# This code sets up a web server on 'localhost:8080'.
# The paths defined for the web server will send messages
# to a queue in us-west-2 called "SQS_IN". If it does not
# exist for you, then the queue is created.
# NOTE: Although the actions are different, each message
# will tell you which action is intended ('action' string), 
# and what it is attempting to act on ('id' string).

# PS: Message bodies seem to turn into giant chains of
# gibberish in the SQS console. Please check the actual
# message attributes for the meaningful data.

import boto.sqs
import argparse
from bottle import Bottle, run, route, request, response, template
from boto.sqs.message import Message

#Globals
AWS_REGION = "us-west-2"
app = Bottle()


'''
	Build the parsed arguments, made a function incase we want to add more
'''
def build_parser():
	parser = argparse.ArgumentParser()
	parser.add_argument("in_queue", help="name of sqs input queue")
	return parser
    
def main():
	global args
	parser = build_parser()
	print parser.parse_args()
	args = parser.parse_args()
	#Connect to (or create) the IN_QUEUE
	conn = boto.sqs.connect_to_region(AWS_REGION)
	in_queue = conn.create_queue(args.in_queue)
	run(app, host='localhost', port=8080)
	

### BEGIN @ROUTE DEFINITIONS ###

@app.route('/create')

def create_user():
	user_id = request.query.id
	name = request.query.name
	activities = request.query.activities

	create_body = 'Creating user with id: %s' % user_id

	create_message = Message()
	create_message.set_body(create_body)
	create_message.message_attributes = {
		"action": {
			"data_type": "String",
			"string_value": "create"
		},
		"id": {
			"data_type": "String",
			"string_value": user_id
		},
		"name": {
			"data_type": "String",
			"string_value": name
		},
		"activities": {
			"data_type": "String",
			"string_value": activities
		}
	}

	#Get this message into the queue
	in_queue.write(create_message)
	return create_message.message_attributes

@app.route('/retrieve')

def retrieve_user():
	user_id = request.query.id
	name = request.query.name

	retrieve_message = Message()

	if (user_id):
		retrieve_body = 'Retrieving user with id: %s' % user_id
		retrieve_message.message_attributes = {
			"action": {
				"data_type": "String",
				"string_value": "retrieve"
			},
			"id": {
				"data_type": "String",
				"string_value": user_id
			}
		}

	elif (name):
		retrieve_body = 'Retrieving user with name: %s' % name
		retrieve_message.message_attributes = {
			"action": {
				"data_type": "String",
				"string_value": "retrieve"
			},
			"name": {
				"data_type": "String",
				"string_value": name
			}
		}

	else:
		return "Try again!"

	retrieve_message.set_body(retrieve_body)

	#Get this message into the queue
	in_queue.write(retrieve_message)
	return retrieve_message.message_attributes

@app.route('/delete')

def delete_user():
	user_id = request.query.id
	name = request.query.name

	delete_message = Message()

	if (user_id):
		delete_body = 'Deleting user with id: %s' % user_id
		delete_message.message_attributes = {
			"action": {
				"data_type": "String",
				"string_value": "delete"
			},
			"id": {
				"data_type": "String",
				"string_value": user_id
			}
		}

	elif (name):
		delete_body = 'Deleting user with name: %s' % name
		delete_message.message_attributes = {
			"action": {
				"data_type": "String",
				"string_value": "delete"
			},
			"name": {
				"data_type": "String",
				"string_value": name
			}
		}

	else:
		return "Try again!"

	delete_message.set_body(delete_body)

	in_queue.write(delete_message)
	return delete_message.message_attributes

@app.route('/add_activities')

def add_activities():
	user_id = request.query.id
	activities = request.query.activities

	add_activities_body = 'Adding to user with id: %s' % user_id

	add_activities_message = Message()
	add_activities_message.set_body(add_activities_body)
	add_activities_message.message_attributes = {
		"action": {
			"data_type": "String",
			"string_value": "add_activities"
		},
		"id": {
			"data_type": "String",
			"string_value": user_id
		},
		"activities": {
			"data_type": "String",
			"string_value": activities
		}
	}

	#Get this message into the queue
	in_queue.write(add_activities_message)
	return add_activities_message.message_attributes

 ### END OF @ROUTE DEFINITIONS ###


if __name__ == "__main__":
    main()
