#!/usr/bin/env python

# Initial thoughts:
# Algorithm is to accept incoming sequence number (or possibly a tuple, storing the operation behind the number), then add that into a heap.
# Then the current sequence number of the operation in database is checked against the smallest value of the heap.
# If the smallest value is the next sequence number from the current sequence number, that sequence number is removed from the heap and the operation ran, while increasing the current sequence number by one

# The incoming sequence number is from the subscribe stream
# The current sequence number should be be kept to keep track of what sequence number the database has performed, such that the operations are all performed in sequence

import heapq

# This function adds the incoming sequence number (Could be tuple later) into the heap
# Since the database doesn't really care which database the operation is coming from, it stores all the sequence numbers into the same heap, following just the sequence numbers. That is, the sequence number from both publish and subscribe should be in this heap.
# To be called when something happens in publish/subscribe
def add_seq_num(heap, seqNum):
	heapq.heappush(heap, seqNum)
	heap.sort()

# This function looks at the smallest number in heap, and checks if it is the next sequence number compared to the sequence number of the last operation that was performed. If so, it pops the sequence number off the heap and performs the associated operation, increasing the sequence number of the last performed operation by one
def compare_seq_num(heap, lastSeqNum):
	if (len(heap) < 1):
		return lastSeqNum
	elif (lastSeqNum + 1) == heap[0]: #If the next value is indeed the next sequence number
		newSeqNum = heapq.heappop(heap) #Pop the value from the heap and make it the current
		heap.sort()
		return newSeqNum
	elif lastSeqNum == heap[0]: #Else if it is a duplicate
		heapq.heappop(heap) #Just remove it from the heap
		heap.sort()
		return lastSeqNum
	else: #Wait until the next sequence number is there
		return lastSeqNum

'''
Method to use Algorithm
#On database initalize:
	seq_hash = []
	current_seq_num = 0

#On incoming publish/subscribe:
	#To do: Find method to get new_seq_num from publish/subscribe
	new_seq_num = (Get from publish/subscribe)
	add_seq_num(seq_hash, new_seq_num)

#Checking to see if can run next operation
	compared_num = compare_seq_num(seq_hash, current_seq_num)
	if (compared_num > current_seq_num):
		run operation
		current_seq_num++
	else:
		Do nothing
'''
