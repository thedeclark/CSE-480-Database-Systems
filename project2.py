"""
Name: Austin DeClark
Time To Completion: 10 hours
Comments: Selecting and ordering were the hardest parts of the project other than that things went pretty smoothly.

Sources: https://stackoverflow.com/questions/3121979/how-to-sort-list-tuple-of-lists-tuples-by-the-element-at-a-given-index
"""
import string
from operator import itemgetter
from tokenizer import tokenizer

_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        self.database = Database()

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def create_table(tokens):
            prev_token = ""
            col_flag = False
            temp_holder = []
            name = ""
            temp = None
            while tokens:
                if tokens[0] == "CREATE":
                    tokens = tokens[1:]
                    continue
                if tokens[0] == "TABLE":
                    prev_token = tokens[0]
                    tokens = tokens[1:]
                    continue
                if prev_token == "TABLE":
                    prev_token = tokens[0]
                    name = tokens[0]
                    tokens = tokens[1:]
                    continue
                if tokens[0] == "(":
                    col_flag = True
                    prev_token = tokens[0]
                    tokens = tokens[1:]
                    continue
                if col_flag == True and tokens[0] == ",":
                    tokens = tokens[1:]
                    continue
                if col_flag == True and tokens[0] != ")":
                    col_name = tokens[0]
                    col_type = tokens[1]
                    temp = (col_name, col_type)
                    temp_holder.append(temp)
                    tokens = tokens[2:]
                    continue
                if tokens[0] == ")":
                    col_flag = False
                    prev_token = tokens[0]
                    tokens = tokens[1:]
                    continue
                if tokens[0] == ";":
                    new_table = Table(name, temp_holder)
                    self.database.add_table(name,new_table)

                    tokens = tokens[1:]
                    break
                    
        def insert(tokens):
            table_name = ""
            prev_token = ""
            flag = False
            add_list = []
            while tokens:
                if tokens[0] == "INSERT":
                    tokens = tokens[1:]
                    continue
                if tokens[0] == "INTO":
                    prev_token = tokens[0]
                    tokens = tokens[1:]
                    continue
                if prev_token == "INTO":
                    table_name = tokens[0]
                    prev_token = tokens[0]
                    tokens = tokens[1:]
                    continue
                if tokens[0] == "VALUES":
                    tokens =tokens[1:]
                    continue
                if tokens[0] == "(":
                    flag = True
                    tokens = tokens[1:]
                    continue
                if flag == True and tokens[0] == ",":
                    tokens = tokens[1:]
                    continue
                if flag == True and tokens[0] != ")":
                    add_list.append(tokens[0])
                    tokens = tokens[1:]
                    continue
                if flag == True and tokens[0] == ")":
                    tokens = tokens[1:]
                    flag = False
                    continue
                if tokens[0] == ";":
                    tokens = tokens[1:]
                    self.database.tables[table_name].add_row(table_name, add_list)
                    break
        def select(tokens):
            table_name = ""
            prev_token = ""
            col_names = []
            order_by = []
            select_flag = False
            all_flag = False
            order_flag = False
            while tokens:
                if tokens[0] == "SELECT":
                    tokens = tokens[1:]
                    prev_token = "SELECT"
                    select_flag = True
                    continue
                if prev_token == "SELECT" and tokens[0] != "*":
                    col_names.append(tokens[0])
                    prev_token = tokens[0]
                    tokens = tokens[1:]
                    continue
                if prev_token == "SELECT" and tokens[0] == "*":
                    prev_token = "*"
                    tokens = tokens[1:]
                    all_flag = True
                    continue
                if tokens[0] == "FROM":
                    select_flag = False
                    prev_token = "FROM"
                    tokens = tokens[1:]
                    continue
                if select_flag == True and tokens[0] == ",":
                    tokens = tokens[1:]
                    continue
                if select_flag == True and tokens[0] != ",":
                    col_names.append(tokens[0])
                    prev_token = tokens[0]
                    tokens = tokens[1:]
                    continue
                if prev_token == "FROM":
                    table_name = tokens[0]
                    prev_token = tokens[0]
                    tokens = tokens[1:]
                    continue
                if tokens[0] == "ORDER":
                    tokens = tokens[1:]
                    continue
                if tokens[0] == "BY":
                    tokens = tokens[1:]
                    prev_token = "BY"
                    order_flag = True
                    continue
                if tokens[0] == ";":
                    tokens = tokens[1:]
                    return self.database.tables[table_name].select_from(col_names, order_by, all_flag)
                if order_flag and tokens[0] != ",":
                    order_by.append(tokens[0])
                    tokens = tokens[1:]
                    continue
                if order_flag and tokens[0] == ",":
                    tokens = tokens[1:]
                    continue
                    
                if tokens[0] == ";":
                    tokens = tokens[1:]
                    return self.database.tables[table_name].select_from(col_names, order_by)

        
        tokens = tokenizer(statement)

        if tokens[0] == "CREATE":
            create_table(tokens)
        if tokens[0] == "INSERT":
            insert(tokens)
        if tokens[0] == "SELECT":
            return select(tokens)
            


    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    def __init__(self):
        self.tables = {}
        
    def add_table(self, name, table):
        self.tables[name] = table

class Table:
    def __init__(self,name, schema):
        self.name = name
        self.rows = []
        self.schema = schema
        
        
    def select_from(self,cols,order_by,all_flag):
        order_ind = []
        ind = []
        return_list = []
        tup = ()
        if all_flag == False:
            for j in cols:
                for i in self.schema:
                    if i[0] == j:
                        temp_tup = (self.schema.index(i),i[1],j)
                        ind.append(temp_tup)
        if all_flag == True:
            for i in self.schema:
                    temp_tup = (self.schema.index(i),i[1],i[0])
                    ind.append(temp_tup)
        for i in self.rows:
            tup = ()
            for j in order_by:
                for k in ind:
                    if j == k[2] and k[0] not in order_ind:
                        index = ind.index(k)
                        order_ind += [index]
                    if k[1] == "INTEGER":
                        tup2 = (int(i[k[0]]),)
                        if tup2[0] not in tup: 
                            tup += tup2
                            continue
                    if k[1] == "TEXT":
                        tup2 = (str(i[k[0]]),)
                        if tup2[0] not in tup: 
                            tup += tup2
                            continue
                    if k[1] == "REAL":
                        if i[k[0]] == "NULL":
                            tup2 = (None,)
                            if tup2[0] not in tup: 
                                tup += tup2
                                continue
    
                            
                        tup2 = (float(i[k[0]]),)
                        if tup2[0] not in tup: 
                            tup += tup2
                            continue
                      
            return_list.append(tup)
        print(order_ind)
        print(return_list)
        if len(order_ind) == 1:
            return_list = sorted(return_list, key=lambda tup: tup[order_ind[0]])
        #This sorting method I used is from the page provided above
        if len(order_ind) == 2:
            return_list = sorted(return_list, key=lambda tup: (tup[order_ind[0]],tup[order_ind[1]]))
        return return_list
                    
    def add_row(self, name, row):
        self.rows.append(row)
    


