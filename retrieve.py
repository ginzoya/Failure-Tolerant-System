'''
  Mock retrieve for final project boilerplate, CMPT 474, Spring 2015.

  This file simulates what a retrieve might do against a real DynamoDB
  database.

  The mock table is created in table.py.
'''

def do_retrieve(table, name, id, request_count):
    if id in table:
        return table[id]
    else:
        return False
