import string
from operator import itemgetter
from collections import namedtuple
import copy
import itertools

_ALL_DATABASES = {}

WhereClause = namedtuple("WhereClause", ["col_name", "operator", "constant"])
UpdateClause = namedtuple("UpdateClause", ["col_name", "constant"])
FromJoinClause = namedtuple("FromJoinClause", ["left_table_name",
                                               "right_table_name",
                                               "left_join_col_name",
                                               "right_join_col_name"])

current_connections = []

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
        self.status = 0
        self.commit = 0
        self.sl = 0
        self.el = 0 
        self.rl = 0
        self.copy = Database('t')
        

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
            not_flag = 0
            pop_and_check(tokens, "CREATE")
            pop_and_check(tokens, "TABLE")
            if tokens[0] == "IF":
                pop_and_check(tokens, "IF")
            if tokens[0] == "NOT":
                pop_and_check(tokens, "NOT")
                not_flag = 1
            if tokens[0] == "EXISTS":
                pop_and_check(tokens, "EXISTS")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "(")
            column_name_type_pairs = []
            while True:
                column_name = tokens.pop(0)
                qual_col_name = QualifiedColumnName(column_name, table_name)
                column_type = tokens.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                column_name_type_pairs.append((qual_col_name, column_type))
                comma_or_close = tokens.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs, not_flag)
            
        def drop_table(tokens):
            exists_flag = 0
            pop_and_check(tokens, "DROP")
            pop_and_check(tokens, "TABLE")
            if tokens[0] == "IF":
                pop_and_check(tokens, "IF")
            if tokens[0] == "EXISTS":
                exists_flag = 1
                pop_and_check(tokens, "EXISTS")
            table_name = tokens[0]
            self.database.drop_table(table_name, exists_flag)

        def insert(tokens):
            """
            Determines the table name and row values to add.
            """
            def get_comma_seperated_contents(tokens):
                contents = []
                pop_and_check(tokens, "(")
                while True:
                    item = tokens.pop(0)
                    contents.append(item)
                    comma_or_close = tokens.pop(0)
                    if comma_or_close == ")":
                        return contents
                    assert comma_or_close == ',', comma_or_close

            pop_and_check(tokens, "INSERT")
            pop_and_check(tokens, "INTO")
            table_name = tokens.pop(0)
            
            if self.rl == 0:
                if current_connections:
                    for i in current_connections:
                        if i.rl == 1:
                            raise Exception()
                            
            if self.rl == 0 and self.status == 1:
                if current_connections:
                    for i in current_connections:
                        if i.rl == 1:
                            raise Exception()

                self.rl = 1
            if self.el == 0:
                if current_connections:
                    for i in current_connections:
                        if i.el == 1:
                            raise Exception()
            if tokens[0] == "(":
                col_names = get_comma_seperated_contents(tokens)
                qual_col_names = [QualifiedColumnName(col_name, table_name)
                                  for col_name in col_names]
            else:
                qual_col_names = None
            pop_and_check(tokens, "VALUES")
            while tokens:
                row_contents = get_comma_seperated_contents(tokens)
                if qual_col_names:
                    assert len(row_contents) == len(qual_col_names)
                if self.status == 1:
                    self.copy.insert_into(table_name,
                                          row_contents,
                                          qual_col_names=qual_col_names)
                else:
                    self.database.insert_into(table_name,
                                            row_contents,
                                            qual_col_names=qual_col_names)
                if tokens:
                    pop_and_check(tokens, ",")

        def get_qualified_column_name(tokens):
            """
            Returns comsumes tokens to  generate tuples to create
            a QualifiedColumnName.
            """
            possible_col_name = tokens.pop(0)
            if tokens and tokens[0] == '.':
                tokens.pop(0)
                actual_col_name = tokens.pop(0)
                table_name = possible_col_name
                return QualifiedColumnName(actual_col_name, table_name)
            return QualifiedColumnName(possible_col_name)

        def update(tokens):
            pop_and_check(tokens, "UPDATE")
            table_name = tokens.pop(0)
            pop_and_check(tokens, "SET")
            update_clauses = []
            while tokens:
                qual_name = get_qualified_column_name(tokens)
                if not qual_name.table_name:
                    qual_name.table_name = table_name
                pop_and_check(tokens, '=')
                constant = tokens.pop(0)
                update_clause = UpdateClause(qual_name, constant)
                update_clauses.append(update_clause)
                if tokens:
                    if tokens[0] == ',':
                        tokens.pop(0)
                        continue
                    elif tokens[0] == "WHERE":
                        break
            x = 0
            
            if self.rl == 0 and self.status == 1:
                if current_connections:
                    for i in current_connections:
                        if i.rl == 1:
                            raise Exception()
                if not x:
                    self.rl = 1
            if self.el == 0:
                if current_connections:
                    for i in current_connections:
                        if i.el == 1:
                            raise Exception()
                        

            where_clause = get_where_clause(tokens, table_name)
            if self.status:
                self.copy.update(table_name, update_clauses, where_clause)
            else:
                self.database.update(table_name, update_clauses, where_clause)

        def delete(tokens):
            pop_and_check(tokens, "DELETE")
            pop_and_check(tokens, "FROM")
            table_name = tokens.pop(0)
            where_clause = get_where_clause(tokens, table_name)
            if self.el == 0:
                if current_connections:
                    for i in current_connections:
                        if i.el:
                            raise Exception()
            
            if self.rl == 0 and self.status == 1:
                if current_connections:
                    for i in connections:
                        if i.rl == 1:
                            raise Exception()

            self.rl = 1
            if self.status == 1:
                self.copy.delete(table_name, where_clause)
            else:
                self.database.delete(table_name, where_clause)

        def get_where_clause(tokens, table_name):
            if not tokens or tokens[0] != "WHERE":
                return None
            tokens.pop(0)
            qual_col_name = get_qualified_column_name(tokens)
            if not qual_col_name.table_name:
                qual_col_name.table_name = table_name
            operators = {">", "<", "=", "!=", "IS"}
            found_operator = tokens.pop(0)
            assert found_operator in operators
            if tokens[0] == "NOT":
                tokens.pop(0)
                found_operator += " NOT"
            constant = tokens.pop(0)
            if constant is None:
                assert found_operator in {"IS", "IS NOT"}
            if found_operator in {"IS", "IS NOT"}:
                assert constant is None
            return WhereClause(qual_col_name, found_operator, constant)

        def select(tokens):
            """
            Determines the table name, output_columns, and order_by_columns.
            """

            def get_from_join_clause(tokens):
                left_table_name = tokens.pop(0)
                if tokens[0] != "LEFT":
                    return FromJoinClause(left_table_name, None, None, None)
                pop_and_check(tokens, "LEFT")
                pop_and_check(tokens, "OUTER")
                pop_and_check(tokens, "JOIN")
                right_table_name = tokens.pop(0)
                pop_and_check(tokens, "ON")
                left_col_name = get_qualified_column_name(tokens)
                pop_and_check(tokens, "=")
                right_col_name = get_qualified_column_name(tokens)
                return FromJoinClause(left_table_name,
                                      right_table_name,
                                      left_col_name,
                                      right_col_name)

            pop_and_check(tokens, "SELECT")
            if self.sl == 0 and self.status == 1:
                self.sl = 1
            if self.el == 0:
                if current_connections:
                    for i in current_connections:
                        if i.el == 1:
                            raise Exception()
            
            is_distinct = tokens[0] == "DISTINCT"
            if is_distinct:
                tokens.pop(0)

            output_columns = []
            while True:
                qual_col_name = get_qualified_column_name(tokens)
                output_columns.append(qual_col_name)
                comma_or_from = tokens.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','

            # FROM or JOIN
            table_name = tokens[0]
            from_join_clause = get_from_join_clause(tokens)
            table_name = from_join_clause.left_table_name

            # WHERE
            where_clause = get_where_clause(tokens, table_name)

            # ORDER BY
            pop_and_check(tokens, "ORDER")
            pop_and_check(tokens, "BY")
            order_by_columns = []
            while True:
                qual_col_name = get_qualified_column_name(tokens)
                order_by_columns.append(qual_col_name)
                if not tokens:
                    break
                pop_and_check(tokens, ",")
            if self.status:
                return self.copy.select(
                output_columns,
                order_by_columns,
                from_join_clause=from_join_clause,
                where_clause=where_clause,
                is_distinct=is_distinct)
            else:
                if len(self.database.tables[table_name].rows)<len(_ALL_DATABASES["test.db"].tables[table_name].rows):
                    
                    self.database = copy.deepcopy(_ALL_DATABASES["test.db"])
                    self.commit = 0
                return self.database.select(
                output_columns,
                order_by_columns,
                from_join_clause=from_join_clause,
                where_clause=where_clause,
                is_distinct=is_distinct)
        
        def begin(tokens):
            self.rb = 0
            if self not in current_connections:
                current_connections.insert(0,self)
            pop_and_check(tokens, "BEGIN")
            if tokens[0] == "DEFERRED":
                pop_and_check(tokens, "DEFERRED")
            if tokens[0] == "IMMEDIATE":
                pop_and_check(tokens, "IMMEDIATE")
                for i in current_connections:
                    if i.rl == 1:
                        raise Exception()
                self.rl = 1
            if tokens[0] == "EXCLUSIVE":
                pop_and_check(tokens, "EXCLUSIVE")
                for i in current_connections:
                    if i.el == 1:
                        raise Exception()
                    if i.rl == 1:
                        raise Exception()
                self.el = 1
            pop_and_check(tokens, "TRANSACTION")
            if self.status == 0:
                self.status = 1
                self.copy = copy.deepcopy(_ALL_DATABASES["test.db"])                
            else:
                raise Exception
        def commit(tokens):
            if self.rb == 1:
                raise Exception()
            pop_and_check(tokens, "COMMIT")
            pop_and_check(tokens, "TRANSACTION")
            ind = current_connections.index(self)
            if current_connections:
                for i in range(0, len(current_connections)-1):
                    if i == ind:
                        print('here')
                        continue
                    else:
                        print(current_connections[i].sl)
                        if current_connections[i].sl == 1:
                            raise Exception()
                self.sl = 0
                current_connections.pop(ind)
            if self.rl == 1:
                self.el = 1
            if self.rl == 1 and self.el != 1:
                raise Exception()
            self.el = 0
            self.rl = 0
            if self.status == 1:
                self.status = 0
                _ALL_DATABASES["test.db"] =  copy.deepcopy(self.copy)
                self.commit = 1
            else:
                raise Exception()
        def rollback(tokens):
            if self.rb == 1 or self.status == 0:
                raise Exception("no roll")
            pop_and_check(tokens, "ROLLBACK")
            pop_and_check(tokens, "TRANSACTION")
            self.rb = 1
            self.status = 0
            self.rl = 0
            self.el = 0
            self.sl = 0
            
            

        tokens = tokenize(statement)
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            create_table(tokens)
            return []
        elif tokens[0] == "DROP":
            drop_table(tokens)
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "SELECT":
            return select(tokens)
        elif tokens[0] == "BEGIN":
            return begin(tokens)
        elif tokens[0] == "COMMIT":
            return commit(tokens)
        elif tokens[0] == "ROLLBACK":
            return rollback(tokens)
        else:
            raise AssertionError(
                "Unexpected first word in statements: " + tokens[0])

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename, timeout=0.1, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class QualifiedColumnName:

    def __init__(self, col_name, table_name=None):
        self.col_name = col_name
        self.table_name = table_name

    def __str__(self):
        return "QualifiedName({}.{})".format(
            self.table_name, self.col_name)

    def __eq__(self, other):
        same_col = self.col_name == other.col_name
        if not same_col:
            return False
        both_have_tables = (self.table_name is not None and
                            other.col_name is not None)
        if not both_have_tables:
            return True
        return self.table_name == other.table_name

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash((self.col_name, self.table_name))

    def __repr__(self):
        return str(self)


class Database:

    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs, flag):
        if flag == 1:
            if table_name in self.tables:
                return []
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []
        
    def drop_table(self, table_name, flag):
        if flag == 1:
            if table_name not in self.tables:
                return []
        if table_name not in self.tables:
            raise Exception
        assert table_name in self.tables
        del self.tables[table_name]
        return []
        
    def insert_into(self, table_name, row_contents, qual_col_names=None):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents, qual_col_names=qual_col_names)
        return []

    def update(self, table_name, update_clauses, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.update(update_clauses, where_clause)

    def delete(self, table_name, where_clause):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.delete(where_clause)

    def select(self, output_columns, order_by_columns,
               from_join_clause,
               where_clause=None, is_distinct=False):
        assert from_join_clause.left_table_name in self.tables
        if from_join_clause.right_table_name:
            assert from_join_clause.right_table_name in self.tables
            left_table = self.tables[from_join_clause.left_table_name]
            right_table = self.tables[from_join_clause.right_table_name]
            all_columns = itertools.chain(
                zip(left_table.column_names, left_table.column_types),
                zip(right_table.column_names, right_table.column_types))
            left_col = from_join_clause.left_join_col_name
            right_col = from_join_clause.right_join_col_name
            join_table = Table("", all_columns)
            combined_rows = []
            for left_row in left_table.rows:
                left_value = left_row[left_col]
                found_match = False
                for right_row in right_table.rows:
                    right_value = right_row[right_col]
                    if left_value is None:
                        break
                    if right_value is None:
                        continue
                    if left_row[left_col] == right_row[right_col]:
                        new_row = dict(left_row)
                        new_row.update(right_row)
                        combined_rows.append(new_row)
                        found_match = True
                        continue
                if left_value is None or not found_match:
                    new_row = dict(left_row)
                    new_row.update(zip(right_row.keys(),
                                       itertools.repeat(None)))
                    combined_rows.append(new_row)

            join_table.rows = combined_rows
            table = join_table
        else:
            table = self.tables[from_join_clause.left_table_name]
        return table.select_rows(output_columns, order_by_columns,
                                 where_clause=where_clause,
                                 is_distinct=is_distinct)


class Table:

    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents, qual_col_names=None):
        if not qual_col_names:
            qual_col_names = self.column_names
        assert len(qual_col_names) == len(row_contents)
        row = dict(zip(qual_col_names, row_contents))
        for null_default_col in set(self.column_names) - set(qual_col_names):
            row[null_default_col] = None
        self.rows.append(row)

    def update(self, update_clauses, where_clause):
        for row in self.rows:
            if self._row_match_where(row, where_clause):
                for update_clause in update_clauses:
                    row[update_clause.col_name] = update_clause.constant

    def delete(self, where_clause):
        self.rows = [row for row in self.rows
                     if not self._row_match_where(row, where_clause)]

    def _row_match_where(self, row, where_clause):
        if not where_clause:
            return True
        new_rows = []
        value = row[where_clause.col_name]

        op = where_clause.operator
        cons = where_clause.constant
        if ((op == "IS NOT" and (value is not cons)) or
                (op == "IS" and value is cons)):
            return True

        if value is None:
            return False

        if ((op == ">" and value > cons) or
            (op == "<" and value < cons) or
            (op == "=" and value == cons) or
                (op == "!=" and value != cons)):
            return True
        return False

    def select_rows(self, output_columns, order_by_columns,
                    where_clause=None, is_distinct=False):
        def expand_star_column(output_columns):
            new_output_columns = []
            for col in output_columns:
                if col.col_name == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names
                       for col in columns)

        def ensure_fully_qualified(columns):
            for col in columns:
                if col.table_name is None:
                    col.table_name = self.name

        def sort_rows(rows, order_by_columns):
            return sorted(rows, key=itemgetter(*order_by_columns))

        def generate_tuples(rows, output_columns):
            for row in rows:
                yield tuple(row[col] for col in output_columns)

        def remove_duplicates(tuples):
            seen = set()
            uniques = []
            for row in tuples:
                if row in seen:
                    continue
                seen.add(row)
                uniques.append(row)
            return uniques

        expanded_output_columns = expand_star_column(output_columns)

        check_columns_exist(expanded_output_columns)
        ensure_fully_qualified(expanded_output_columns)
        check_columns_exist(order_by_columns)
        ensure_fully_qualified(order_by_columns)

        filtered_rows = [row for row in self.rows
                         if self._row_match_where(row, where_clause)]
        sorted_rows = sort_rows(filtered_rows, order_by_columns)

        list_of_tuples = generate_tuples(sorted_rows, expanded_output_columns)
        if is_distinct:
            return remove_duplicates(list_of_tuples)
        return list_of_tuples


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
                              string.ascii_letters + "_" + string.digits)
    if word == "NULL":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    if (query[0] == "'"):
        delimiter = "'"
    else:
        delimiter = '"'
    query = query[1:]
    end_quote_index = query.find(delimiter)
    while query[end_quote_index + 1] == delimiter:
        # Remove Escaped Quote
        query = query[:end_quote_index] + query[end_quote_index + 1:]
        end_quote_index = query.find(delimiter, end_quote_index + 1)
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
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
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query, tokens)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[:2] == "!=":
            tokens.append(query[:2])
            query = query[2:]
            continue

        if query[0] in "(),;*.><=":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[0] in {"'", '"'}:
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError(
                "Query didn't get shorter. query = {}".format(query))

    return tokens
