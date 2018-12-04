from playhouse.dataset import DataSet
import zerorpc


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


srv = zerorpc.Server(Service())
srv.bind('tcp://0.0.0.0:4242')
srv.run()


"""
TODO:
- bring the client code here
- setup the server inside if __main__ block
- use argparse to get binding address
"""
