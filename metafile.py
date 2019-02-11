METADATA = 'files/metadata.txt'

class MetaFile(dict):
    def __init__(self):
        dict.__init__(self)
        self.filename = METADATA
        self.read()

    def read(self):
        myFile = open(self.filename, 'r')
        isNewTable = False

        for line in myFile:
            l = line.strip()
            if l=="<begin_table>":
                isNewTable = True
            elif isNewTable:
                tableName = l
                self[tableName] = []
                isNewTable = False
            elif l != "<end_table>":
                self[tableName].append(l)