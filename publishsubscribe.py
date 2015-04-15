#!/usr/bin/env python
'''
	The purpose of this script is to take in an instance's sequence
	and operation and publish it to other instances.
'''

#imports from frontend.py, not sure if I need them
from frontend import get_ports
'''
	Takes gathers the sequence number and operation of an instance
	and returns a tuple. 
	NOTE: not sure at the moment where I get the sequence number and operation.
	According to the diagram, I would gather it from the DBInstance, so I need
	to wait for Aaron to implement it? Discuss later.
'''
def get_seq_and_operation:
	portTuple = get_ports() #tuple with publisher port and list of subscriber ports
	#need to get the sequence number and operation, store it in tuple
	#seqNumber = dbInstnace.getSeqNumber()
	#operation = dbInstance.getOperation()
	#return seqNumber, operation
	

