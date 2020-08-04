"""
Name: Dennis Phillips
Time To Completion: 3 hours
Comments:

Sources:
"""
import string
from operator import itemgetter

_ALL_DATABASES = {}


class Connection(object):
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def create_table(tokens):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            while True:
                column_name = tokens.pop(0)
                    
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            columns = []
            while True:
                
                if tokens[0] == "VALUES":
                    break
                elif tokens[0] == "(" or tokens[0] == ")" or tokens[0] == ",":
                    tokens.pop(0)
                    continue
                else:
                    columns.append(tokens[0])
                    tokens.pop(0)
            

            pop_and_check(tokens, "VALUES")
            pop_and_check(tokens, "(")
            row_contents = []
            rows =[]
            while tokens:

                item = tokens.pop(0)
                row_contents.append(item)
                comma_or_close = tokens.pop(0)
                if tokens:
                    if tokens[0] == ";":
                        rows.append(row_contents)
                        break
                    elif tokens[0] == ",":
                        tokens.pop(0)
                        tokens.pop(0)
                        rows.append(row_contents)
                        row_contents = []
                else:
                    rows.append(row_contents)
                    break
                
            self.database.insert_into(table_name, rows, columns)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            pop_and_check(tokens, "SELECT")
            output_columns = []
            clause = []
            d_flag = False
            on_clause = []
            qual_table = None
            if tokens[0] == "DISTINCT":
                tokens.pop(0)
                d_flag = True
            while True:
                col = tokens.pop(0)
                if "." in col:
                    ind = col.find(".")
                    qual_col = col[ind+1:]
                    qual_table = col[:ind]
                
                output_columns.append((qual_col,qual_table))
                print(output_columns)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','
            table_name = tokens.pop(0)
            if tokens[0] == "LEFT":
                tokens.pop(0)
                tokens.pop(0)
                tokens.pop(0)
                right_table = tokens.pop(0)
                tokens.pop(0)
                on_clause.append(tokens.pop(0))
                on_clause.append(tokens.pop(0))
                on_clause.append(tokens.pop(0))
            print(on_clause)
                
            if tokens[0] == "WHERE":
                tokens.pop(0)
                col_name = tokens.pop(0)
                if "." in col_name:
                    ind = col_name.find(".")
                    qual_col = col_name[ind+1:]
                    qual_table = col_name[:ind]

                assert qual_col in self.database.tables[table_name].rows[0]
                clause.append((qual_col,qual_table))
                while tokens[0] != "ORDER":
                    clause.append(tokens.pop(0))
                
                    
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            while True:
                col = tokens.pop(0)
                if "." in col:
                    ind = col.find(".")
                    qual_col = col[ind+1:]
                    qual_table = col[:ind]
                order_by_columns.append((qual_col,qual_table))
                if not tokens:
                    break
                pop_and_check(tokens, ",")
            return self.database.select(
                output_columns, table_name, order_by_columns, clause, d_flag, on_clause)
                
        def delete(tokens):
            clause = []
            col_name = ""
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens,  "FROM")
            table_name = tokens.pop(0)
            if tokens:
                pop_and_check(tokens, "WHERE")
                col_name = tokens.pop(0)
                if tokens[0] != "IS":
                    clause.append(tokens.pop(0))
                    clause.append(tokens.pop(0))
                else:
                    while tokens:
                        clause.append(tokens.pop(0))
            
            self.database.delete_from(table_name, col_name, clause)
        
        def update(tokens):
            updates = []
            list_ = []
            clause = []
            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")
            while tokens:
                list_.append(tokens.pop(0))
                list_.append(tokens.pop(0))
                list_.append(tokens.pop(0))
                
                if not tokens:
                    updates.append(list_)
                    list_ = []
                    break
                else:
                    if tokens[0] == ",":
                        tokens.pop(0)
                        updates.append(list_)
                        list_ = []
                    elif tokens[0] == "WHERE":
                        tokens.pop(0)
                        clause.append(tokens.pop(0))
                        clause.append(tokens.pop(0))
                        if clause[1] == "IS":
                            if tokens[0] != "NOT":
                                clause.append(tokens.pop(0))
                                updates.append(list_)
                            else:
                                clause.append(tokens.pop(0))
                                clause.append(tokens.pop(0))
                                updates.append(list_)
                        else:
                            clause.append(tokens.pop(0))
                            updates.append(list_)
                        
            self.database.update(table_name, updates, clause)
            
                

        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        else:  # tokens[0] == "SELECT"
            return select(tokens)
        assert not tokens
        

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
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def insert_into(self, table_name, row_contents, columns):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents, columns)
        return []

    def select(self, output_columns, table_name, order_by_columns, clause, d_flag, on_clause):
        new = []
        right_col = []
        print(output_columns)
        assert table_name in self.tables
        for i in output_columns:
            if i[1] != table_name:
                right_table_name = i[1]
                continue
            if i[1] == table_name:
                new.append(i[0])
                continue
        print(new)
        left_table = self.tables[table_name]
        return left_table.select_rows(new, order_by_columns, clause, d_flag)
        
    def delete_from(self, table_name, col_name, clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete_rows(col_name, clause)
        
    def update(self, table_name, updates, clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update(updates, clause)


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []
        
    def delete_rows(self, col_name, clause):
        counts = []
        if not clause:
            self.rows = []
        else:
            if len(clause) == 2:
                if clause[0] == "IS":
                    count = 0
                    for row in self.rows:
                        if row[col_name] == None:
                            self.rows.pop(count)
                        count += 1
                if clause[0] == "<":
                    count = 0
                    for row in self.rows:
                        if row[col_name] is None:
                            count+=1
                            continue
                        if row[col_name] < clause[1]:
                            counts.append(count)
                        count += 1
                    for c in reversed(counts):
                        self.rows.pop(c)
                        
    def update(self, updates, clause):
        if not clause:
            for update in updates:
                col_name = update[0]
                cond = update[1]
                end = update[2]
                for row in self.rows:
                    row[col_name] = end
                    
        else:
            for update in updates:
                col_name = update[0]
                cond = update[1]
                end = update[2]
                if len(clause) == 3:
                    clause_col_name = clause[0]
                    clause_cond  = clause[1]
                    clause_end = clause[2]
                if len(clause) == 4:
                    clause_col_name = clause[0]
                    clause_cond = "IS NOT"
                    clause_end = clause[3]
                if clause_cond == ">":
                    for row in self.rows:
                        if row[col_name] is not None:
                            if row[clause_col_name] > clause_end:
                                row[col_name] = end
                if clause_cond == "<":
                    for row in self.rows:
                        if row[col_name] is not None:
                            if row[clause_col_name] < clause_end:
                                row[col_name] = end

                if clause_cond == "=":
                    for row in self.rows:
                        if row[col_name] is not None:
                            if row[clause_col_name] == clause_end:
                                row[col_name] = end
                
                if clause_cond == "!=":
                    for row in self.rows:
                        if row[col_name] is not None:
    
                            if row[clause_col_name] != clause_end:
                                row[col_name] = end
                
                if clause_cond == "IS":
                    for row in self.rows:
                        if row[clause_col_name] is None:
                            row[col_name] = end
                            
                if clause_cond == "IS NOT":
                    for row in self.rows:
                        if row[clause_col_name] is not None:
                            row[col_name] = end


    def insert_new_row(self, row_contents, columns):
        if columns:
            actual_rows = []
            for j in range(len(row_contents)):
                actual_row_contents = []
                for i in self.column_names:
                    if i in columns:
                        ind = columns.index(i)
                        actual_row_contents.append(row_contents[j][ind])
                    else:
                        actual_row_contents.append(None)
                actual_rows.append(actual_row_contents)

            for t in actual_rows:
                row = dict(zip(self.column_names, t))
                self.rows.append(row)
        else:
            for t in row_contents:
                row = dict(zip(self.column_names, t))

                self.rows.append(row)


    def select_rows(self, output_columns, order_by_columns, clause, d_flag):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col == "*":
                    new_output_columns.extend(self.column_names)

                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            print(columns)
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns):
            return sorted(self.rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns, clause):
            cond = ""
            actual_list_ = []
            list_ =[]
            distinct = []
            if not clause:
                for row in rows:
                    tup = ()
                    temp = ()
                    for col in output_columns:
                        if d_flag:
                            if row[col] not in distinct:
                                temp += (row[col],)
                                distinct.append(row[col])
                            else:
                                continue
                        else:
                            temp += (row[col],)
                    print(distinct)
                    tup += (temp)
                    if len(tup) > 0:
                        actual_list_.append(tup)
                        list_ = []
                    
                return actual_list_
            
            if len(clause) == 3:
                col_name = clause.pop(0)
                cond = clause.pop(0)
                end = clause.pop(0)

                
            if len(clause) == 4:
                col_name = clause[0]
                for row in rows:
                    tup = ()
                    if row[col_name] is not None:
                        temp = ()
                        for col in output_columns:
                            if d_flag:
                                if rol[col] not in distinct:
                                    distinct.append(row[col])
                                    temp += (row[col],)
                                else:
                                    continue
                            else:
                                temp += (row[col],)
                        tup += (temp)
                        actual_list_.append(tup)
                        list_ = []
                return actual_list_
            if cond == ">":
                for row in rows:
                    tup = ()
                    if row[col_name] is not None:
                        if row[col_name] > end:
                            temp = ()
                            for col in output_columns:
                                if d_flag:
                                    if rol[col] not in distinct:
                                        distinct.append(row[col])
                                        temp += (row[col],)
                                    else:
                                        continue
                                else:
                                    temp += (row[col],)
                            tup += (temp)
                            actual_list_.append(tup)
                            list_ = []
                    else:
                        continue
                return actual_list_
                
            if cond == "<":
                for row in rows:
                    tup = ()
                    if row[col_name] is not None:
                        if row[col_name] < end:
                            temp = ()
                            for col in output_columns:
                                if d_flag:
                                    if row[col] not in distinct:
                                        distinct.append(row[col])
                                        temp += (row[col],)
                                    else:
                                        continue
                                else:
                                    temp += (row[col],)
                            tup += (temp)
                            actual_list_.append(tup)
                            list_ = []
                    else:
                        continue
                return actual_list_
                
            if cond == "IS":
                for row in rows:
                    tup = ()
                    if row[col_name] is None:
                        temp = ()
                        for col in output_columns:
                            if d_flag:
                                if row[col] not in distinct:
                                    distinct.append(row[col])
                                    temp += (row[col],)
                                else:
                                    continue
                            else:
                                temp += (row[col],)
                        tup += (temp)
                        actual_list_.append(tup)
                        list_ = []
                return actual_list_
                
            if cond == "=":
                for row in rows:
                    tup = ()
                    if row[col_name] is not None:
                        if row[col_name] == end:
                            temp = ()
                            for col in output_columns:
                                if d_flag:
                                    if row[col] not in distinct:
                                        distinct.append(row[col])
                                        temp += (row[col],)
                                    else:
                                        continue
                                else:
                                    temp += (row[col],)
                            tup += (temp)
                            actual_list_.append(tup)
                            list_ = []
                    else:
                        continue
                return actual_list_
                
            if cond == "!=":
                for row in rows:
                    tup = ()
                    if row[col_name] is not None:
                        if row[col_name] != end:
                            temp = ()
                            for col in output_columns:
                                if d_flag:
                                    if row[col] not in distinct:
                                        distinct.append(row[col])
                                        temp += (row[col],)
                                    else:
                                        continue
                                else:
                                    temp += (row[col],)
                            tup += (temp)
                            actual_list_.append(tup)
                            print(actual_list_)
                            list_ = []
                    else:
                        continue
                return actual_list_

        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)
        check_columns_exist(order_by_columns)
        sorted_rows = sort_rows(order_by_columns)


        return generate_tuples(sorted_rows, expanded_output_columns,clause)


def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query, tokens):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query,
                              string.ascii_letters + "_" + "." + string.digits + "<" + ">" + "=" + "!" +"*")
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]
    


def remove_text(query, tokens):
    assert query[0] == "'"
    query = query[1:]
    text = ''
    flag = False
    count = 0
    e_f = False
    s_flag = False
    space_flag = False
    for i in query:
        if flag == True:
            flag = False
            count+=1
            continue
        if (i == "\'") and (query[count+1] == " "):
            space_flag = True
            break
        
        if (i == "\'") and (query[count+1] == ";"):
            s_flag = True
            break
            
        if (i == "\'") and (query[count+1] != "," and query[count+1] != ")"):
            flag = True
            text += i
            count+=1
            continue

        if i == "\'" and (query[count+1] == "," or query[count+1] == ")"):
            if query[count+1] == ")":
                e_f = True
            break
        text += i
        count+=1
    if s_flag == True:
        end_quote_index = query.find(";")
        tokens.append(text)
        query = query[end_quote_index:]
        return query
    if space_flag == True:
        end_quote_index = query.find(" ")
        tokens.append(text)
        query = query[end_quote_index:]

        if query[0] == " " and query[1:6] != "ORDER":
            query = query[1:]
            end_quote_index = query.find(" ")
            query = query[end_quote_index:]
        
        return query
    if e_f == False:
        end_quote_index = query.find(",")
        tokens.append(text)
        query = query[end_quote_index:]
    else:
        end_quote_index = query.find(")")
        tokens.append(text)
        query = query[end_quote_index:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        #print("Query:{}".format(query))
        #print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_" + "<" + ">" + "=" + "!"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] == "'":
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")
    

    return tokens
    

conn = connect("test.db")
conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, class TEXT);")
conn.execute("CREATE TABLE classes (course TEXT, instructor TEXT);")
conn.execute("INSERT INTO students VALUES ('Josh', 99, 'CSE480'), ('Dennis', 99, 'CSE480'), ('Jie', 52, 'CSE491');")
conn.execute("INSERT INTO students VALUES ('Cam', 56, 'CSE480'), ('Zizhen', 56, 'CSE491'), ('Emily', 74, 'CSE431');")
conn.execute("INSERT INTO classes VALUES ('CSE480', 'Dr. Nahum'), ('CSE491', 'Dr. Josh'), ('CSE431', 'Dr. Ofria');")

x = conn.execute("SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course ORDER BY classes.instructor, students.name, students.grade;")
print(list(x))


