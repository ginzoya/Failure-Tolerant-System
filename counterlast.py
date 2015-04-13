"""Zookeeper CounterLast

:Maintainer: Ted Kirkpatrick
:Status: Modified for CMPT 474, Spring 2015

"""

#from kazoo.exceptions import BadVersionError
#from kazoo.retry import ForceRetryError

from kazoo.exceptions import BadVersionError
from kazoo.retry import ForceRetryError
from kazoo.recipe.counter import Counter

class CounterLast(Counter):
    """Kazoo CounterLast

    Extend kazoo.recipe.Counter with a field, last_set. The
    field contains the most recent value to which this client
    set the counter.

    CounterLast changes can raise
    :class:`~kazoo.exceptions.BadVersionError` if the retry policy
    wasn't able to apply a change.

    Example usage:

    .. code-block:: python

        zk = KazooClient()
        counter = zk.CounterLast("/int")
        counter += 2
        counter -= 1
        counter.last_set == 1
        counter.value == 1 # Assuming no concurent changes

        counter = zk.CounterLast("/float", default=1.0)
        counter += 2.0
        counter.last_set == 3.0
        counter.value == 3.0 # Assuming no concurrent changes

        zk = KazooClient()
        counter = zk.CounterLast("/int")
        counter += 1
        # Other process does counter += 1 
        counter.last_set == 1 # Value as of last set in this client
        counter.value == 2 # Most recent value on server
        

    """
    def __init__(self, client, path, default=0):
        """Create a Kazoo Counter

        :param client: A :class:`~kazoo.client.KazooClient` instance.
        :param path: The counter path to use.
        :param default: The default value.

        """
        Counter.__init__(self, client, path, default)
        self._last_set = b''

    @property
    def last_set(self):
        last = self._last_set.decode('ascii') if self._last_set != b'' else self.default
        return self.default_type(last)

    def _inner_change(self, value):
        data, version = self._value()
        data = repr(data + value).encode('ascii')
        try:
            self.client.set(self.path, data, version=version)
            self._last_set = data
        except BadVersionError:  # pragma: nocover
            raise ForceRetryError()
