#!/usr/bin/python

import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.dynamodb2.fields import HashKey
from dbinstance import create, retrieve_id, retrieve_name, add, delete_id, delete_name

# Test driver for dbinstance.py
# NOTE: This runs dbinstance, so you'll need to Ctrl+C it before the test code runs.

# [DEBUG] Create Table
def debug_create_table():
	users = Table.create('users', 
		schema=[
	    	HashKey('id'),
	    ], 
	    throughput={
		    'read': 5,
		    'write': 15,
		},
		connection=boto.dynamodb2.connect_to_region('us-west-2')
		)
	print "Table created!"
	return

# [DEBUG] Populate Table
def debug_populate_table():
	create("12", "John", "swimming,sleeping")
	create("13", "SolidSnake", "espionage,cqc")
	create("14", "RevolverOcelot", "six,bullets,more,than,enough")
	create("15", "SniperWolf", "sniping,dying,hankerchieves")
	create("16", "Otacon", "anime")
	create("17", "GrayFox", "pain,snake,anime")
	return

def debug_retrieve_users():
	res = retrieve_id("12")
	print res
	res = retrieve_name("SolidSnake")
	print res
	return

def debug_add_activities():
	res = add("16", "hacking,hiding")
	print res
	res = add("15", "waiting")
	print res

def debug_delete_users():
	res = delete_id("12")
	print res
	res = delete_name("RevolverOcelot")
	print res

# [DEBUG] Delete Table
def debug_delete_table():
	try:
		users = Table('users', connection=boto.dynamodb2.connect_to_region('us-west-2'))
		Table.delete(users)
		print "Deleting users!"
	except Exception, c:
		print c
	return

# [DEBUG] Test driver to connect and populate the database
# Comment and uncomment sections as necessary
def debug_test():

	# debug_create_table()

	try:
		users = Table('users', connection=boto.dynamodb2.connect_to_region('us-west-2'))
	except Exception, e:
		print e
	
	# debug_populate_table()

	# debug_retrieve_users()

	# debug_add_activities()

	# debug_delete_users()

	# debug_delete_table()

	return

debug_test()