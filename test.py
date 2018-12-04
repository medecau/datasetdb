from collections import namedtuple
from functools import partial
import zerorpc


table = namedtuple('Table', 'insert update delete find find_one all'.split(' '))
class Table:
    def __init__(self, c, name):
        self.c = c
        self.name = name

    def insert(self, **kwargs):
        return self.c.insert(self.name, kwargs)

    def update(self, **kwargs):
        return self.c.update(self.name, kwargs)

    def delete(self, **kwargs):
        return self.c.delete(self.name, kwargs)

    def find(self, **kwargs):
        return self.c.find(self.name, kwargs)

    def find_one(self, **kwargs):
        return self.c.find_one(self.name, kwargs)

    def all(self):
        return self.c.all(self.name)

    def __iter__(self):
        return self.all()


class Client:
    def __init__(self, address=None):
        if address is None:
            address = 'tcp://127.0.0.1:4242'
        self.c = zerorpc.Client(address)

    def __getitem__(self, name):
        return Table(self.c, name)


c = Client()
