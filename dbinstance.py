#!/usr/bin/python

import json
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER

# Code for a database instance. Imported code from assignment 3 and aggregated it to this file for centralized usage
# TODO: change how the parameters are obtained, since they're supposed to come from requests

# Code from grayfox.py
#Create a new user if not defined. Else: Return that user entry.
#Takes an id (str), name (str), and activities (str)
def create(input_id="", input_name="", input_activities=""):
	# [TODO] Error handling, alternative return status codes

	# (Attempt to) Connect to table
	# If the table doesn't exist, feel free to un-comment the creation code at the bottom.
	try:
		users = Table('users', connection=boto.dynamodb2.connect_to_region('us-west-2'))

	except Exception, c:
		print c


	# Converting the input activities to a list, then a set
	activities = set(input_activities.split(','))

	# Preparing the returning JSON
	return_object = {
		"data": {
			"type": "person",
			"id": input_id,
			"links": {
				"self": "http://localhost:8080/retrieve?id=%s" % input_id
			}
		}
	}

	# Assuming success
	return_response = 201

	# Check if specified ID is already present
	try:
		attempted_retrieval = users.get_item(id=input_id)
		print "Found ID!"

	# ID not found, create a new entry
	except Exception, e:
		print e
		print "User ID not found!"
		users.put_item( data={'type':'person', 'id':input_id, 'name':input_name, 'activities': activities})
		print "User %s created successfully!" % input_name

		# User created, exit the function
		return return_response, return_object

	# If we've made it this far, we've got an entry for this ID.
	retrieved_id = attempted_retrieval['id']
	retrieved_name = attempted_retrieval['name']
	retrieved_activities = attempted_retrieval['activities']

	retrieved_list = ', '.join(retrieved_activities)

	# Either the name or activity set does not match
	if ((activities != retrieved_activities) or (input_name != retrieved_name)):
		print "Mismatch found! Activities or name does not match."

		return_object = {
			"errors": [{
			    "id_exists": {
			      	"status": "400",
			      	"title": "id already exists",
			      	"detail": {
			        	"name": retrieved_name,
			        	"activities": retrieved_list
			     	}
			    }
			  }]
		}

		return_response = 400
		return return_response, return_object
	
	# ID found; name and activities match. Do nothing.
	else:
		print "Match found! Everything is OK!"
	
	return return_response, return_object

# Retrieval code from sniper_wolf.py
# Since we no longer have an index on name, we must scan the table when given just the username.
def retrieve(input_id="", input_name=""):
	if (input_id):
		results = retrieve_id(input_id)
	else:
		results = retrieve_name(input_name)
	return results

#Function for retrieval by ID
def retrieve_id(user_id):
	#Grabs the table from dynamodb
	users = Table('users', connection=boto.dynamodb2.connect_to_region('us-west-2'))
	#Attempt to grab the item from the table
	try:
		#Call for getting item from table
		user = users.get_item(id=user_id)

		#Gets the set of activites and changes it to json after converting to list
		activities = user['activities']
		output = json.dumps(list(activities))

		#Report success, and adds the relevant information into a tuple
		result = 200, {
			"data": {
				"type": user['type'],
				"id": str(user['id']),
				"name": user['name'],
				"activities": output
			}
		}
	#Catches the exception of when the item isn't found
	except boto.dynamodb2.exceptions.ItemNotFound:
		#Reports failure, telling the user that the item isn't found
		result = 404, {
			"errors": [{
				"not_found": {
					"id": str(user_id)
				}
			}]
		}
	#Returns the result back to the main file
	return result

#Function for retrieval by username
def retrieve_name(username):
	#Get the table from dynamodb
	users = Table('users', connection=boto.dynamodb2.connect_to_region('us-west-2'))

	#Assuming a failure first, before assigning a success
	result = 404, {
		"errors": [{
			"not_found": {
				"name": username
			}
		}]
	}

	#Scan database for user with specified username
	user_with_name = users.scan(name__eq=username)

	#Assuming there is only one item with this username
	for user in user_with_name:
		#Get the list of activities
		activities = user['activities']
		#Changes the set into a json format after changing to list
		output = json.dumps(list(activities))

		#Returns success and gets information from the item, overwriting the failure
		result = 200, {
			"data": {
				"type": user['type'], #Gets the type of the item
				"id": str(user['id']), #Gets the ID of the item, in string format
				"name": user['name'], #Gets the name of the item
				"activities": output #Assign the list of activites to the returning object
			}
		}
		
	#Returns the result of the retrieval to the main file
	return result

# Add activity code from big_boss.py
#add an activity to an existing id if exists, else return an error
#takes in an id idnum and either a single or a list of activities (single only for 1.0)
#and returns a message with status code 200 for success, and 404 if the id number
#is not found in the table
def add(idnum, activities):
	#get the table
	users = Table('users', connection=boto.dynamodb2.connect_to_region('us-west-2'))
	#split the list by comma
	activity_set = set(activities.split(","))
	#convert set to list so we can dump into a json representation
	activity_list = list(activity_set)
	#get json respresentations of the added activities
	activity_json = json.dumps(activity_list)
	#try to find matching id and add activities
	try:
		user = users.get_item(id=idnum)
		#print "User exists"
		#adds activity list to activities field
		for item in activity_set:
			user['activities'].add(item)
		#saves it
		user.partial_save() #because of concurrent activities, we want to partial save
		response_body = 200, {
				"data": {
				"type": "person",
				"id": str(idnum),
				"added": activity_json
					}
				}
	#failed to find, output 404 message
	except Exception, e:
		#print error in terminal
		print e
		response_body = 404, {
				"errors": [{
				"not_found": {
				"id": str(idnum)
					}
					}]
				}
	#return the tuple for the error code and response message
	return response_body

# Delete user code from revolver_ocelot.py
# Also changed the query to a scan now that there's no index on name
def delete(input_id="", input_name=""):
	if (input_id != ""):
		results = delete_id(input_id)
	else:
		results = delete_name(input_name)
	return results

def delete_id(idnum):
	try:
		users = Table('users', connection=boto.dynamodb2.connect_to_region('us-west-2'))
	except Exception, c:
		print c

	# try and get user based on idnum
	try:
		person = users.get_item(id=str(idnum))
		# grab relevant details before deleting user
		data_type = person['type']

		# try to delete user from users table
		person.delete()

		# return success
		response_body = 200, {
			"data": {
				"type": data_type,
				"id": str(idnum)
			}
		}
	except Exception, e:
		print e
		response_body = 404, {
			"errors":[{
				"notfound": {
					"id":str(idnum)
				}
			}]
		}
	return response_body

def delete_name(username):
	# initialize response body to fail state
	response_body = 404, {
			"errors":[{
				"notfound": {
					"name":username
				}
			}]
		}
	try:
		users = Table('users', connection=boto.dynamodb2.connect_to_region('us-west-2'))
	except Exception, c:
		print c

	# try and get user based on idnum
	try:
		# run a query on username
		query_res = users.scan(name__eq=username)

		# for the assignment, we're guaranteed unique names, so grab only the
		# first person from the query, in case multiple users are matched with the name
		for user in query_res:
			# grab relevant details before deleting user
			data_type = user['type']
			idnum = user['id']

			user.delete()

			# return success
			response_body = 200, {
				"data": {
					"type": data_type,
					"id": str(idnum)
				}
			}
			break;

	except Exception, e:
		print e
	return response_body