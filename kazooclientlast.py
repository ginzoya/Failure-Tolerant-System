"""Extend Kazoo ZooKeeper client to use Counterlast"""

from functools import partial

from counterlast import CounterLast
from kazoo.client import KazooClient

class KazooClientLast(KazooClient):

    def __init__(self, hosts='127.0.0.1:2181',
                 timeout=10.0, client_id=None, handler=None,
                 default_acl=None, auth_data=None, read_only=None,
                 randomize_hosts=True, connection_retry=None,
                 command_retry=None, logger=None, **kwargs):
        KazooClient.__init__(self, hosts,
                             timeout, client_id, handler,
                             default_acl, auth_data, read_only,
                             randomize_hosts, connection_retry,
                             command_retry, logger, **kwargs)
        self.Counter = partial(CounterLast, self)

    
