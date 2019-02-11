import csv
from metafile import MetaFile

metafile = MetaFile()

class Table(list):
    def __init__(self, name):
        list.__init__(self)
        self.name = name
        self.nrows = 0
        self.read()

    def read(self):
        filename = 'files/' + self.name + '.csv'
        with open(filename) as csvfile:
            csvr = csv.reader(csvfile)
            for row in csvr:
                r = {}
                for i in range(len(row)):
                    col_name = metafile[self.name][i]
                    r[col_name] = int(row[i])
                self.append(r)
            self.nrows += 1