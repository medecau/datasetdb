from collections import namedtuple
from functools import partial
import zerorpc

table = namedtuple('Table', 'insert update delete find find_one all'.split(' '))

class Client:
    def __init__(self, address=None):
        if address is None:
            address = 'tcp://127.0.0.1:4242'
        self.c = zerorpc.Client(address)

    def _insert(self, table, **kwargs):
        print(kwargs)
        self.c.insert(table, kwargs)

    def _update(self, table, **kwargs):
        self.c.update(table, kwargs)

    def _delete(self, table, **kwargs):
        self.c.delete(table, kwargs)

    def _find(self, table, **kwargs):
        for record in self.c.find(table, kwargs):
            yield record

    def _find_one(self, table, **kwargs):
        return self.c.find_one(table, kwargs)

    def _all(self, table):
        for record in self.c.all(table):
            yield record

    def __getitem__(self, name):
        return table(
            partial(self._insert, name),
            partial(self._update, name),
            partial(self._delete, name),
            partial(self._find, name),
            partial(self._find_one, name),
            partial(self._all, name)
            )


c = Client()

