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

def send_message(pubPort, message):    
    print "Instance: " + instance + " publishing: " + message
    pubPort.send_string(message)
    return {
        "data": {
            "type": "Message sent"
            }
        }
def receive_message(subPorts):
    if len(subPorts) == 0:
        response.status = 404
        return {
            "errors": [{
                "no subscriptions": {
                    "msg": "This instance is not subscribed to any publishers"
                    }
                }]
            }
    #TODO: finish
	
listOfPorts = gen_ports() #get ports, first is publish, rest is list of subscriber
send_message(listOfPorts[0], message)
receive_message(listOfPorts[1:])
	
