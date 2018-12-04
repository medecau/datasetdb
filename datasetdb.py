from collections import namedtuple
from functools import partial

from playhouse.dataset import DataSet
import zerorpc


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

class Service:
    def __init__(self):
        self.db = DataSet('sqlite+pool:///db.sqlite')

    def insert(self, table, doc):
        return self.db[table].insert(**doc)

    def update(self, table, doc):
        return self.db[table].update(**doc)

    def delete(self, table, doc):
        return self.db[table].delete(**doc)

    @zerorpc.stream
    def find(self, table, doc):
        return self.db[table].find(**doc)

    def find_one(self, table, doc):
        return self.db[table].find_one(**doc)

    @zerorpc.stream
    def all(self, table):
        return self.db[table].all()

if __name__ == '__main__':
    srv = zerorpc.Server(Service())
    srv.bind('tcp://0.0.0.0:4242')
    srv.run()


"""
TODO:
- use argparse to get binding address
"""
