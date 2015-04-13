'''
  Mock DynamoDB table. For testing final projects, CMPT 474, Spring 2015.
'''

def open_table():
    ''' For now, just a mock with two entries '''
    return {
        333: {'name': 'andrew_petter', 'activities': 'presidenting,speechifying'},
        1245: {'name': 'barbara_mcclintock', 'activities': 'cytogenetics,nobel_prize_polishing'}
    }
