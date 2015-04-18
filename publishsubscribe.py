#!/usr/bin/env python
'''
    The purpose of this script is to take in an instance's sequence
    and operation and publish it to other instances.
'''

#imports from frontend.py, not sure if I need them
from frontend import get_ports
import zmq
'''
    Takes gathers the sequence number and operation of an instance
    and returns a tuple. 
    NOTE: Sequence numbers not used, instead receiver will scan
    the list of sockets (5 seconds per entry) for new events.
'''

def send_message(pub_port, message):    
    print "Instance: " + instance + " publishing: " + message
    pub_port.send_string(message)
    return {
        "data": {
            "type": "Message sent"
            }
        }
def receive_message(sub_ports):

    if len(sub_ports) == 0:
        response.status = 404
        return {
            "errors": [{
                "no subscriptions": {
                    "msg": "This instance is not subscribed to any publishers"
                    }
                }]
            }

    else:
        for (index in len(sub_ports)):
            num_events = sub_ports[index].poll(timeout=5)
            if (num_events > 0):
                try:
                    message_content = sub_ports[index].recv(zmq.NOBLOCK)
                    return {
                        "data": {
                            "type": "message",
                            "msg": message_content
                        }
                    }
                except zmq.ZMQError as zerr:
                    if zerr.errno == zmq.EAGAIN:
                        #no messages found
                        return {
                            "data": {
                                "type": "empty_message",
                            }
                        }
                    else:
                        # Not an empty message, but worse
                        raise zerr

    
listOfPorts = gen_ports() #get ports, first is publish, rest is list of subscriber
send_message(listOfPorts[0], message)
receive_message(listOfPorts[1:])
    
