import sys
import copy
import csv
import sqlparse
import traceback

from metafile import MetaFile
from table import Table

class Query:
    def __init__(self, command):
        query = sqlparse.parse(command.strip())[0]

        if str(query.tokens[-1]) != ';':
            raise Exception('Semi Colon not present in the query')

        if str(query.tokens[0]).lower() != "select":
            raise NotImplementedError('Only select query type is supported')

        self.distinct = False
        i = 2
        if str(query.tokens[i]).lower() == 'distinct':
            self.distinct = True
            i += 2
        
        if type(query.tokens[i+4]).__name__ != 'Identifier':
            self.tables = list(query.tokens[i+4].get_identifiers())
            self.tables = [str(x) for x in self.tables]
        else:
            self.tables = [str(query.tokens[i+4])]

        self.validate_tables()
        self.colsaggcol, self.colsaggfn = list(), list()

        if str(query.tokens[i]) != "*":
            if type(query.tokens[i]).__name__ != 'IdentifierList':
                self.cols = [query.tokens[i]]
            else:
                self.cols = list(query.tokens[i].get_identifiers())
            
            colsn = list()
            
            for col in self.cols:
                if type(col).__name__ == 'Function':
                    fn_done = False
                    for token in col.tokens:
                        if type(token).__name__ == 'Identifier':
                            if fn_done:
                                # self.colsaggcol.append(self.proper_col(str(token)))
                                self.colsaggcol = [*self.colsaggcol, self.proper_col(str(token))]
                                break
                            else:
                                # self.colsaggfn.append(str(token).lower())
                                self.colsaggfn = [*self.colsaggfn, str(token).lower()]
                                fn_done = True
                        elif type(token).__name__ == 'Parenthesis':
                            col.tokens += token.tokens[1:-1]
                    colsn.append(self.colsaggfn[-1] + '(' + self.colsaggcol[-1] + ')')
                else:
                    colsn = [*colsn, str(col)]
            
            self.cols = colsn
        else:
            self.cols = [table + '.' + col for table in self.tables for col in metafile[table]]
        
        self.where = list()
        if len(query.tokens) > i+6:
            self.where = query.tokens[i+6].tokens
            if not str(self.where[0]).lower() == "where":
                raise NotImplementedError('Only where is supported' + str(self.where))
            self.where = self.where[2:]
        
        self.validate_cols()
        self.join_tables()
        self.solve_distinct()
        self.solve_where()
        self.solve_aggregate()

    def validate_tables(self):
        for table in self.tables:
            if table not in metafile:
                raise Exception('Invalid table ' + table)

    def proper_col(self, col):
        if '.' in col:
            colx = col.split('.')
            if colx[0] not in self.tables or colx[1] not in metafile[colx[0]]:
                raise Exception('Invalid column ' + col)
            return col

        myTable = None
        for table in self.tables:
            if col in metafile[table]:
                if myTable is not None:
                    raise Exception('Column %s present in multiple tables' % col)
                else:
                    myTable = table
        if myTable is not None:
            return myTable + '.' + col

        raise Exception('Invalid column ' + col)

    def validate_cols(self):
        for col in self.cols:
            if '(' in col:
                continue
            i = self.cols.index(col)
            self.cols[i] = self.proper_col(self.cols[i])

    def join_tables(self):
        self.nt = self.recurse_join(self.tables)

    def recurse_join(self, ttj):
        nts = list()
        table = ttj[0]
        
        if len(ttj) == 1:
            for row in tables[table]:
                rts = {}
                for col in row:
                    rts[table + '.' + col] = row[col]
                nts.append(rts)
            return nts
        
        ots = self.recurse_join(ttj[1:])
        for row in tables[table]:
            for row2 in ots:
                rts = copy.deepcopy(row2)
                for col in row:
                    rts[table + '.' + col] = row[col]
                nts.append(rts)
        return nts

    def solve_distinct(self):
        if not self.distinct:
            return

        s = set()
        nnt = list()
        
        for row in self.nt:
            tp = str([row[col] for col in self.cols])
            if not tp in s:
                nnt.append(row)
                s.add(tp)
        
        self.nt = nnt

    def solve_where(self):
        if len(self.where) == 0:
            return
        
        nnt = list()
        
        for _, row in enumerate(self.nt):
            status = self.test_row(row, self.where)
            if status:
                nnt.append(row)
        
        self.nt = nnt

    def test_row(self, row, clause):
        prev = True
        prevand = True
        
        for _, condition in enumerate(clause):
            ns = None
            
            if str(condition.ttype) == 'Token.Text.Whitespace':
                continue
            elif str(condition.ttype) == 'Token.Keyword':
                if str(condition).lower() == 'or':
                    prevand = False
                elif str(condition).lower() == 'and':
                    prevand = True
            elif type(condition).__name__ == 'Parenthesis':
                ns = self.test_row(row, condition.tokens[1:-1])
            elif type(condition).__name__ == 'Comparison':
                iden1 = None
                iden2 = None
                op = None
                value = None
                tokens = condition.tokens
                for token in tokens:
                    if token.ttype is not None:
                        if str(token.ttype).startswith('Token.Literal'):
                            value = int(str(token))
                        elif str(token.ttype) == 'Token.Operator.Comparison':
                            op = str(token)
                    elif type(token).__name__ == 'Identifier':
                        if iden1 is None:
                            if value is not None:
                                op = self.reverseop(op)
                            iden1 = self.proper_col(str(token))
                        else:
                            iden2 = self.proper_col(str(token))
                
                if iden1 is None:
                    ns = True
                elif iden2 is not None:
                    if iden2 in self.cols:
                        self.cols.remove(iden2)
                    ns = self.applyop(row[iden1], row[iden2], op)
                else:
                    ns = self.applyop(row[iden1], value, op)

            if ns is not None:
                if not prevand:
                    prev = prev or ns
                else:
                    prev = prev and ns
        return prev

    def reverseop(self, op):
        if op == '>=':
            return '<='
        elif op == '<=':
            return '>='
        if op == '>':
            return '<'
        elif op == '<':
            return '>'
        return op

    def applyop(self, v1, v2, op):
        v1, v2 = int(v1), int(v2)
        if op == '<=':
            return v1 <= v2
        elif op == '>=':
            return v1 >= v2
        elif op == '<':
            return v1 < v2
        elif op == '>':
            return v1 > v2
        elif op == '=':
            return v1 == v2
        else:
            raise NotImplementedError(op + ' operator not recognized')

    def solve_aggregate(self):
        if len(self.nt) == 0 or len(self.colsaggfn) == 0:
            return
        
        for i, fn in enumerate(self.colsaggfn):
            col = self.colsaggcol[i]
            fullname = fn + '(' + col + ')'
            v = self.nt[0][col]
            for row in self.nt:
                if fn == 'max':
                    v = max(v, row[col])
                elif fn == 'min':
                    v = min(v, row[col])
                elif fn == 'sum' or fn == 'average' or fn == 'avg':
                    v += row[col]
                else:
                    raise NotImplementedError('Function %s not implemented' % fn)
            if fn == 'avg' or fn == 'average':
                v = round(float(v) / len(self.nt), 2)
            for row in self.nt:
                row[fullname] = v
        
        if len(self.nt) > 1:
            self.nt = [self.nt[0]]

    def print_result(self):
        for col in self.cols:
            print(col, end="\t")
        print()

        for row in self.nt:
            for col in self.cols:
                print(str(row[col]), end="\t\t"),
            print()

metafile = MetaFile()
tables = {}

for table in metafile:
    tables[table] = Table(table)

try:
    q = Query(sys.argv[1])
    q.print_result()
except Exception as err:
    sys.stderr.write('Error: %s\n' % str(err))