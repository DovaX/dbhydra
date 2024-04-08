import pandas as pd
import numpy as np
from typing import Optional, Any
import abc
import time
#xlsx imports
import pathlib

from dbhydra.src.abstract_table import AbstractTable, AbstractSelectable, AbstractJoinable


MONGO_OPERATOR_DICT = {"=": "$eq", ">": "$gt", ">=": "$gte", " IN ": "$in", "<": "$lt", "<=": "$lte", "<>": "$ne"}

POSTGRES_TO_MYSQL_DATA_MAPPING = {
    "int": "int",
    "integer": "int",
    "bigint": "bigint",
    "smallint": "smallint",
    "character varying": "varchar",
    "text": "longtext",
    "boolean": "tinyint",
    "double precision": "double",
    "real": "float",
    "numeric": "decimal",
    "date": "date",
    "timestamp": "timestamp"
}

PYTHON_TO_MYSQL_DATA_MAPPING = {
    'int': "int",
    'float': "double",
    'str': "nvarchar(2047)",
    'bool': "bit",
    'datetime': "datetime"
}




def save_migration(function, *args, **kw):  # decorator
    def new_function(instance, *args, **kw):
        print("TOTO TU")
        print(instance)
        print(*args)
        command = function.__name__
        if command == "create":
            migration_dict = {
                "create": {"table_name": instance.name, "columns": instance.columns, "types": instance.types}}
            print(migration_dict)
        if command == "drop":
            migration_dict = {"drop": {"table_name": instance.name}}
            print(migration_dict)
        if command == "add_column":
            migration_dict = {
                "add_column": {"table_name": instance.name, "column_name": args[0], "column_type": args[1]}}
            print(migration_dict)
        if command == "drop_column":
            migration_dict = {
                "drop_column": {"table_name": instance.name, "column_name": args[0]}}
            print(migration_dict)
        if command == "modify_column":
            migration_dict = {
                "modify_column": {"table_name": instance.name, "column_name": args[0], "column_type": args[1]}}
            print(migration_dict)
        # TODO: add other methods

        migrator = instance.db1.migrator
        migrator.migration_list.append(migration_dict)
        # migrator.migration_list_to_json()
        function(instance, *args, **kw)

    return (new_function)



######### POSTGRES ##############

class PostgresTable(AbstractTable):
    def __init__(self, db1, name, columns=None, types=None):
        super().__init__(db1, name, columns)
        self.types = types

        print("==========================================")

    def initialize_columns(self):
        """
        TODO Dominik: Check for usecases of this method. Isn't it somewhat duplicated by `get_all_columns`?
        """
        columns = self.get_all_columns()
        self.columns = columns

    def initialize_types(self):
        self.types = self.get_all_types()

    def get_all_columns(self):
        information_schema_table = PostgresTable(self.db1, 'INFORMATION_SCHEMA.COLUMNS')
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '{self.name}';"
        columns = information_schema_table.select(query, flattening_of_results=True)

        return columns

    def convert_types_from_mysql(self):
        inverse_dict_mysql_to_postgres = dict(zip(POSTGRES_TO_MYSQL_DATA_MAPPING.values(), POSTGRES_TO_MYSQL_DATA_MAPPING.keys()))
        postgres_types = list(map(lambda x: inverse_dict_mysql_to_postgres.get(x, x), self.types))
        self.types = postgres_types

    def get_all_types(self):
        information_schema_table = PostgresTable(self.db1, 'INFORMATION_SCHEMA.COLUMNS', ['DATA_TYPE'], ['nvarchar(50)'])
        query = "SELECT DATA_TYPE,character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        types = information_schema_table.select(query)
        data_types = [x[0].lower() for x in types]
        date_lengths = [x[1] for x in types]

        mysql_types = list(map(lambda x: POSTGRES_TO_MYSQL_DATA_MAPPING.get(x, x), data_types))


        for i in range(len(mysql_types)):
            if date_lengths[i] is not None:
                mysql_types[i] = mysql_types[i] + f"({date_lengths[i]})"

        return (mysql_types)


    def select_all(self):
        print(super().select_all())
        return [i for i in super().select_all()]

    @classmethod
    def init_all_columns(cls, db1, name, id_column_name="id"):
        temporary_table = cls(db1, name, id_column_name)
        columns = temporary_table.get_all_columns()
        types = temporary_table.get_all_types()

        if temporary_table.id_column_name in columns:
            id_col_index = columns.index(temporary_table.id_column_name)
            columns.pop(id_col_index)
            columns.insert(0, temporary_table.id_column_name)
            types.pop(id_col_index)
            types.insert(0, "int")

        return (cls(db1, name, columns, types))

    # @save_migration
    def create(self, foreign_keys=None):
        assert len(self.columns) == len(self.types)
        assert self.columns[0] == self.id_column_name
        assert self.types[0].lower() == "int" or self.types[0].lower() == "integer"
        query = "CREATE TABLE " + self.name + "("+self.id_column_name+" SERIAL PRIMARY KEY,"
        for i in range(1, len(self.columns)):
            query += self.columns[i] + " " + self.types[i] + ","

        query = query[:-1]
        query += ");"
        print(query)
        try:
            self.db1.execute(query)
        except Exception as e:
            print("Table " + self.name + " already exists:", e)
            print("Check the specification of table columns and their types")

    def insert(self, rows, batch=1, replace_apostrophes=True, try_mode=False, debug_mode=False, insert_id=False):
        start_index = 0 if insert_id else 1
        assert len(self.columns) == len(self.types)
        for k in range(len(rows)):
            if k % batch == 0:
                query = "INSERT INTO " + self.name + " ("
                for i in range(start_index, len(self.columns)):
                    if i < len(rows[k]) + 1:
                        query += self.columns[i] + ","
                if len(rows) < len(self.columns):
                    print(len(self.columns) - len(rows), "columns were not specified")
                if query[-1] == ',':
                    query = query[:-1]
                    query = query + ") VALUES "
                elif query[-1] == '(':
                    query = query[:-1]
                    query = query + " VALUES "



            query += "("
            for j in range(len(rows[k])):
                if rows[k][j] == "NULL" or rows[k][j] == "'NULL'" or rows[k][j] == None or rows[k][
                    j] == "None":  # NaN hodnoty
                    if "int" in self.types[j + start_index]:

                        if replace_apostrophes:
                            rows[k][j] = str(rows[k][j]).replace("'", "")
                        query += "NULL,"
                    else:
                        query += "NULL,"
                elif "nvarchar" in self.types[j + start_index]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "N'" + str(rows[k][j]) + "',"
                elif "varchar" in self.types[j + start_index]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "'" + str(rows[k][j]) + "',"
                elif self.types[j + start_index] == "int":
                    query += str(rows[k][j]) + ","
                elif "datetime" in self.types[j + start_index]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "'" + str(rows[k][j]) + "',"
                elif "date" in self.types[j + start_index]:
                    query += "'" + str(rows[k][j]) + "',"



                else:
                    query += str(rows[k][j]) + ","
            if query[-1] == ",":
                query = query[:-1]
            elif query[-1] == '(':
                query = query + "DEFAULT"

            query = query + "),"
            if k % batch == batch - 1 or k == len(rows) - 1:
                query = query[:-1]

                if debug_mode:
                    print(query)

                if not try_mode:
                    return self.db1.execute(query)
                else:
                    try:
                        return self.db1.execute(query)
                    except Exception as e:

                        print("Query", query, "Could not be inserted:", e)

                        # Write to logs only in debug mode
                        if debug_mode:
                            with open("log.txt", "a") as file:
                                file.write("Query " + str(query) + " could not be inserted:" + str(e) + "\n")

    @save_migration
    def add_column(self, column_name, column_type):
        assert len(column_name) > 1
        command = "ALTER TABLE " + self.name + " ADD COLUMN " + column_name + " " + column_type
        try:
            self.db1.execute(command)
            self.columns.append(column_name)
            self.types.append(column_type)
        except Exception as e:
            print(e)
            print("Cant add column to table.")

    @save_migration
    def drop_column(self, column_name):
        assert len(column_name) > 1
        command = "ALTER TABLE " + self.name + " DROP COLUMN " + column_name
        try:
            self.db1.execute(command)
            index = self.db1.columns.index(column_name)
            self.db1.columns.remove(column_name)
            self.db1.types.remove(self.db1.types[index])
        except Exception as e:
            print(e)
            print("Cant drop " + self.name)

    @save_migration
    def modify_column(self, column_name, new_column_type):
        assert len(column_name) > 1
        command = "ALTER TABLE " + self.name + " ALTER COLUMN " + column_name + " TYPE " + new_column_type
        try:
            self.db1.execute(command)
            index = self.columns.index(column_name)
            self.types[index] = new_column_type
        except Exception as e:
            print("Cant add column to table.")

class BigQueryTable(AbstractSelectable):
    def __init__(self, db1, name, columns=None, types=None):
        self.name = name
        self.db1 = db1
        self.columns = columns
        self.types = types

    @classmethod
    def init_all_columns(cls, db1, name, id_column_name="id"):
        temporary_table = cls(db1, name, id_column_name)
        columns,types = temporary_table.get_all_columns_and_types()

        return (cls(db1, name, columns, types))

    def get_all_columns_and_types(self):
        results = self.db1.client.get_table(f"{self.db1.credentials.project_id}.{self.db1.dataset}.{self.name}")
        column_names = [x.name for x in results.schema ]
        column_types = [x.field_type for x in results.schema]
        return column_names, column_types


    def select(self, query):

        """given SELECT query returns Python list"""
        """Columns give the number of selected columns"""
        print(query)
        # rows =  self.db1.client.query(query).result()
        rows = self.db1.execute(query)
        return rows


############ MONGO ############

class MongoTable():
    def __init__(self, db, name, columns=[], types=[]):
        self.name = name
        self.db1 = db
        print("==========================================")
        print(type(db))
        print(db)
        self.columns = columns
        self.types = types
        self.collection = self.db1.create_table(name)

    def create(self):
        pass

    def drop(self):
        return self.collection.drop()

    def update_collection(self):
        self.collection = self.db1.create_table(self.name)

    def insert(self, document):
        return self.collection.insert_one(document)

    def insert_more(self, documents):
        return self.collection.insert_many(documents)

    def insertMore(self, documents):
        print("WARNING: `insertMore` method will be deprecated in favor of `insert_more`")
        return self.insert_more(documents)

    def select(self, query, columns={}):

        if columns == '*':
            columns = {}

        if (len(columns) == 0):

            return list(self.collection.find(query))
        else:
            return list(self.collection.find(query, columns))

    def select_all(self, query={}):
        return list(self.collection.find(query))

    def select_sort(self, query, fieldname, direction, columns={}):
        if (len(columns) == 0):
            return list(self.collection.find(query).sort(fieldname, direction))
        else:
            return list(self.collection.find(query, columns).sort(fieldname, direction))

    def selectSort(self, query, fieldname, direction, columns={}):
        print("WARNING: `selectSort` method will be deprecated in favor of `select_sort`")
        return self.select_sort(query, fieldname, direction, columns)

    def delete(self, query={}):
        self.collection = self.db1.create_table(self.name)
        return self.collection.delete_many(query)

    def update(self, newvalues, query):
        return self.collection.update_many(query, newvalues)

    def insert_from_df(self, df, insert_id=None):
        if df.empty:
            return
        df = df.replace({pd.NA: None})
        dict_from_df = df.to_dict('records')
        # dict_from_df = dataframe.apply(lambda x : x.dropna().to_dict(),axis=1).tolist() #get rid of nans
        return self.collection.insert_many(dict_from_df)

    def insertFromDataFrame(self, dataframe):
        print("WARNING: `insertFromDataFrame` method will be deprecated in favor of `insert_from_df`")
        return self.insert_from_df(dataframe)

    def select_to_df(self, query={}):
        print(type(pd.DataFrame(list(self.collection.find(query)))))
        print(pd.DataFrame(list(self.collection.find(query))))
        return pd.DataFrame(list(self.collection.find(query)))

    @classmethod
    def init_all_columns(cls, db1, name, id_column_name="id"):
        temporary_table = cls(db1, name, id_column_name)
        values = temporary_table.get_columns_types()
        columns = values[0][1:]
        types = values[1][1:]

        if temporary_table.id_column_name in columns:
            index = columns.index(temporary_table.id_column_name)
            columns.pop(index)
            types.pop(index)

        columns.insert(0, temporary_table.id_column_name)
        types.insert(0, "int")
        types = [x.lower() for x in types]
        types_ = [PYTHON_TO_MYSQL_DATA_MAPPING[x] for x in types]
        types = types_
        return (cls(db1, name, columns, types))

    def keys_exists(self, element, *keys):
        '''
        Check if *keys (nested) exists in `element` (dict).
        '''
        if not isinstance(element, dict):
            raise AttributeError('keys_exists() expects dict as first argument.')
        if len(keys) == 0:
            raise AttributeError('keys_exists() expects at least two arguments, one given.')

        _element = element
        for key in keys:
            try:
                _element = _element[key]
            except KeyError:
                return False
        return True

    def print_nested_keys(self, d, columns, types, parent=""):

        for k in d.keys():

            typ = type(d.get(k)).__name__

            if self.keys_exists(types, k, typ):
                types[k][typ] = types[k][typ] + 1
            else:
                try:
                    types[k][typ] = 1
                except:
                    types[k] = {}
                    types[k][typ] = 1
            if parent + k not in columns:
                columns.append(parent + k)
            if type(d[k]) == dict:
                self.print_nested_keys(d[k], columns, types, parent + k + ".")

    def get_columns_types(self):
        columns = []
        types = {}
        for dict_j in self.collection.find():
            self.print_nested_keys(dict_j, columns, types)
        types = self.get_all_types(types)
        self.columns = columns
        self.types = types
        return columns, types

    def get_all_types(self, types):
        print(types)
        types_list = []
        for k in types.keys():
            values = types.get(k)
            if (len(values) == 1):

                types_list.append(next(iter(values)))
            else:
                chosen = ""
                chosen_cnt = 0
                for t in values.keys():
                    if values.get(t) > chosen_cnt:
                        chosen = t
                types_list.append(chosen + " ?")
        print(types_list)
        return types_list


############## SQL SERVER ##########################


class SqlServerTable(AbstractTable):
    def __init__(self, db1, name, columns=None, types=None):
        """Override joinable init"""
        super().__init__(db1, name, columns)
        self.types = types

    @classmethod
    def init_all_columns(cls, db1, name, id_column_name="id"):
        temporary_table = cls(db1, name, id_column_name)
        columns = temporary_table.get_all_columns()
        types = temporary_table.get_all_types()
        return (cls(db1, name, columns, types))

    def get_all_columns(self):
        information_schema_table = SqlServerTable(self.db1, 'INFORMATION_SCHEMA.COLUMNS')
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        columns = information_schema_table.select(query, flattening_of_results=True)
        return (columns)

    def get_all_types(self):
        information_schema_table = SqlServerTable(self.db1, 'INFORMATION_SCHEMA.COLUMNS', ['DATA_TYPE'], ['nvarchar(50)'])
        query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        types = information_schema_table.select(query)
        return (types)

    def create(self):
        assert len(self.columns) == len(self.types)
        assert self.columns[0] == self.id_column_name
        assert self.types[0].lower() == "int"
        query = "CREATE TABLE " + self.name + "("+self.id_column_name+" INT IDENTITY(1,1) NOT NULL,"
        for i in range(1, len(self.columns)):
            query += self.columns[i] + " " + self.types[i] + ","
        query += "PRIMARY KEY("+self.id_column_name+"))"

        print(query)
        try:
            self.db1.execute(query)
        except Exception as e:
            print("Table " + self.name + " already exists:", e)
            print("Check the specification of table columns and their types")

    def insert(self, rows, batch=1, replace_apostrophes=True, try_mode=False, debug_mode=False):

        assert len(self.columns) == len(self.types)
        for k in range(len(rows)):
            if k % batch == 0:
                query = "INSERT INTO " + self.name + " ("
                for i in range(1, len(self.columns)):
                    if i < len(rows[k]) + 1:
                        query += self.columns[i] + ","
                if len(rows) < len(self.columns):
                    print(len(self.columns) - len(rows), "columns were not specified")
                query = query[:-1] + ") VALUES "

            query += "("

            for j in range(len(rows[k])):
                if rows[k][j] == "NULL" or rows[k][j] == "'NULL'" or rows[k][j] == None or rows[k][
                    j] == "None":  # NaN hodnoty
                    query += "NULL,"
                elif "nvarchar" in self.types[j + 1]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "N'" + str(rows[k][j]) + "',"
                elif "varchar" in self.types[j + 1]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "'" + str(rows[k][j]) + "',"
                elif self.types[j + 1] == "int":
                    query += str(rows[k][j]) + ","
                elif "date" in self.types[j + 1]:
                    query += "'" + str(rows[k][j]) + "',"
                elif "datetime" in self.types[j + 1]:
                    query += "'" + str(rows[k][j]) + "',"
                else:
                    query += str(rows[k][j]) + ","

            query = query[:-1] + "),"
            if k % batch == batch - 1 or k == len(rows) - 1:
                query = query[:-1]

                if debug_mode:
                    print(query)

                if not try_mode:
                    self.db1.execute(query)
                else:
                    try:
                        self.db1.execute(query)
                    except Exception as e:

                        print("Query", query, "Could not be inserted:", e)

                        # Write to logs only in debug mode
                        if debug_mode:
                            with open("log.txt", "a") as file:
                                file.write("Query " + str(query) + " could not be inserted:" + str(e) + "\n")

    def get_foreign_keys_for_table(self, table_dict, foreign_keys):
        # table_dict is in format from db function: generate_table_dict()
        # foreign_keys are in format from db function: get_foreign_keys_columns()
        parent_foreign_keys = []
        for i, fk in enumerate(foreign_keys):
            if fk["parent_table"] == self.name:

                try:
                    print(fk["parent_column_id"])
                    print(self.columns)
                    print(self.name)
                    fk["parent_column_name"] = self.columns[fk["parent_column_id"]]
                    fk["referenced_column_name"] = table_dict[fk["referenced_table"]].columns[
                        fk["referenced_column_id"]]
                    parent_foreign_keys.append(fk)
                except IndexError as e:
                    print("Warning: IndexError for foreign key self.columns[fk[parent_column_id]]:", e)
        return (parent_foreign_keys)


class Table(SqlServerTable):
    """Deprecated - do not remove until dbhydra 3.x"""
    def __init__(self, db1, name, columns=None, types=None):
        print("Deprecation warning!, Table was renamed to SqlServerTable and the old name will deprecated in future!")
        super().__init__(db1, name, columns, types)
        self.types = types
        
    # TODO: New implementation - DISABLED until fixed (expects incorrect arguments thus causing crashes)
    # def __init__(self, config_file="config.ini", db_details=None):
    #     print("Deprecation warning!, Table was renamed to SqlServerTable and the old name will deprecated in future!")
    #     super().__init__(config_file=config_file, db_details=db_details)



########### MYSQL #############



class MysqlTable(AbstractTable):
    def __init__(self, db1, name, columns=None, types=None, id_column_name = "id"):
        super().__init__(db1, name, columns, types)
        self.id_column_name = id_column_name

    #Disabled because it is inherited
    # @classmethod
    # def init_from_column_type_dict(cls, db1, name, column_type_dict, id_column_name="id"):
    #     column_converted_type_dict = db1._convert_column_type_dict_from_python(column_type_dict)
    #     columns = list(column_converted_type_dict.keys())
    #     types = list(column_converted_type_dict.values())
    #     return cls(db1, name, columns, types, id_column_name=id_column_name)

    def initialize_columns(self):
        """
        TODO Dominik: Check for usecases of this method. Isn't it somewhat duplicated by `get_all_columns`?
        """
        columns = self.get_all_columns()
        self.columns = columns

    def convert_types_from_mysql(self):
        pass

    def initialize_types(self):
        self.types = self.get_all_types()

    def get_all_columns(self):
        information_schema_table = MysqlTable(self.db1, 'INFORMATION_SCHEMA.COLUMNS')
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{self.db1.DB_DATABASE}' AND TABLE_NAME  = '{self.name}';"
        columns = information_schema_table.select(query, flattening_of_results=True)

        return columns

    def get_all_types(self):
        data_types, data_lengths = self.get_data_types_and_character_lengths()
        for i in range(len(data_types)):
            if data_lengths[i] is not None:
                data_types[i] = data_types[i] + f"({data_lengths[i]})"
        return (data_types)


    """
        Returns a list of data types, where each element represents the category of the data ('varchar', 'int', etc.). 
        If a data type has an associated length, the length value will be included in a corresponding element of the
        data_lengths list, otherwise the element will have a None value. For example, 'nvarchar(2047)' would return
        'varchar' in the data_types list and 2047 in the data_lengths list.
    """
    def get_data_types_and_character_lengths(self):
        information_schema_table = MysqlTable(self.db1, 'INFORMATION_SCHEMA.COLUMNS', ['DATA_TYPE'], ['nvarchar(50)'])
        query = f"SELECT DATA_TYPE,character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{self.db1.DB_DATABASE}' AND TABLE_NAME  = '{self.name}'"
        types = information_schema_table.select(query)
        data_types = [x[0] for x in types]
        data_lengths = [x[1] for x in types]

        return data_types,data_lengths

    def get_converted_python_types(self):
        SQL_TO_PYTHON = {v: k for k, v in PYTHON_TO_MYSQL_DATA_MAPPING.items()}
        python_types = []
        for type in self.types:
            if "varchar" in type:
                python_types.append("str")
            elif type in SQL_TO_PYTHON:
                python_types.append(SQL_TO_PYTHON[type])
            else:
                raise Exception("Unsupported type")

        return python_types

    def extract_last_id(self) -> Any:
        """
        Extract the last inserted ID from the DB.

        LAST_INSERT_ID exists in the DB connection context, therefore is safe to use if DB session is request-scoped
        In this case we only use global connection, but we use Lock to ensure thread-safety across different requests
        This is a go-to mechanism for extracting the ID of the inserted record for multiple SQL DBs,
        altho this specific query is applicable only to MySQL.
        """
        assert self.name == self.db1.last_table_inserted_into, "Last table inserted into is not the same as the table being queried"
        return self.select("SELECT LAST_INSERT_ID()")[0][0]

    def get_nullable_columns(self):
        information_schema_table = MysqlTable(self.db1, 'INFORMATION_SCHEMA.COLUMNS')
        query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '{self.db1.DB_DATABASE}' and TABLE_NAME = '{self.name}' and IS_NULLABLE = 'YES'"
        nullable_columns = information_schema_table.select(query)
        return (nullable_columns)


    @classmethod
    def init_all_columns(cls, db1, name, id_column_name="id"):
        temporary_table = cls(db1, name, id_column_name)
        columns = temporary_table.get_all_columns()
        types = temporary_table.get_all_types()

        if temporary_table.id_column_name in columns:
            id_col_index = columns.index(temporary_table.id_column_name)
            columns.pop(id_col_index)
            columns.insert(0, temporary_table.id_column_name)
            types.pop(id_col_index)
            types.insert(0, "int")



        return (cls(db1, name, columns, types))

    def get_last_id(self):
        """
        Returns the biggest id from table
        """

        last_id = self.select(f"SELECT {self.id_column_name} FROM {self.name} ORDER BY {self.id_column_name} DESC LIMIT 1;")

        return last_id[0][0]

    def get_num_of_records(self):
        """
        Returns the number of records in table
        """

        num_of_records = self.select(f"SELECT COUNT(*) FROM `{self.name}`;")

        return num_of_records[0][0]

    def drop(self):
        query = "DROP TABLE `" + self.name + "`;"
        print(query)
        self.db1.execute(query)

    # @save_migration #TODO: Uncomment
    def create(self, foreign_keys=None):
        assert len(self.columns) == len(self.types)
        assert self.columns[0] == self.id_column_name
        assert self.types[0].lower() == "int"

        column_type_pairs = list(zip(self.columns, self.types))[1:]
        fields = ", ".join(
            [f"`{column}` {type_.upper()}" for column, type_ in column_type_pairs]
        )
        query = f"CREATE TABLE `{self.name}` ({self.id_column_name} INT UNSIGNED AUTO_INCREMENT PRIMARY KEY, {fields})"

        print(query)
        try:
            self.db1.execute(query)
        except Exception as e:
            print("Table " + self.name + " already exists:", e)
            print("Check the specification of table columns and their types")

    def insert(self, rows, batch=1, replace_apostrophes=True, try_mode=False, debug_mode=False, insert_id=False):
        start_index = 0 if insert_id else 1
        assert len(self.columns) == len(self.types)
        total_output=[]
        for k in range(len(rows)):
            if k % batch == 0:
                query = "INSERT INTO `" + self.name + "` ("
                for i in range(start_index, len(self.columns)):
                    if i < len(rows[k]) + 1:
                        # column name containing space/reserved keyword needs to be wrapped in `...`, otherwise causes syntax error
                        column_name = '`' + self.columns[i] + '`'
                        query += column_name + ","
                if len(rows) < len(self.columns):
                    print(len(self.columns) - len(rows), "columns were not specified")
                if query[-1] == ',':
                    query = query[:-1]
                query = query + ") VALUES "

            query += "("
            for j in range(len(rows[k])):
                if rows[k][j] == "NULL" or rows[k][j] == "'NULL'" or rows[k][j] == None or rows[k][
                    j] == "None":  # NaN hodnoty
                    if "int" in self.types[j + start_index]:

                        if replace_apostrophes:
                            rows[k][j] = str(rows[k][j]).replace("'", "")
                        query += "NULL,"
                    else:
                        query += "NULL,"
                elif "nvarchar" in self.types[j + start_index]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "N'" + str(rows[k][j]) + "',"
                elif "varchar" in self.types[j + start_index]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "'" + str(rows[k][j]) + "',"
                elif "char" in self.types[j + start_index]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "'" + str(rows[k][j]) + "',"
                elif self.types[j + start_index] == "int":
                    query += str(rows[k][j]) + ","
                elif "datetime" in self.types[j + start_index]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "'" + str(rows[k][j]) + "',"
                elif "date" in self.types[j + start_index]:
                    query += "'" + str(rows[k][j]) + "',"
                elif "json" in self.types[j + start_index]:
                    query += f"'{rows[k][j]}', "


                else:
                    query += str(rows[k][j]) + ","
            if query[-1] == ",":
                query = query[:-1]

            query = query + "),"
            if k % batch == batch - 1 or k == len(rows) - 1:
                query = query[:-1]

                if debug_mode:
                    print(query)

                if not try_mode:
                    output=self.db1.execute(query)
                    total_output.append(output)
                else:
                    try:
                        output=self.db1.execute(query)
                        total_output.append(output)
                    except Exception as e:

                        print("Query", query, "Could not be inserted:", e)

                        # Write to logs only in debug mode
                        if debug_mode:
                            with open("log.txt", "a") as file:
                                file.write("Query " + str(query) + " could not be inserted:" + str(e) + "\n")
            
            elif len(total_output)==1:
                return(total_output[0])
            else:
                return(total_output)
                

    def add_foreign_key(self, foreign_key):
        parent_id = foreign_key['parent_id']
        parent = foreign_key['parent']
        query = "ALTER TABLE `" + self.name + "` MODIFY " + parent_id + " INT UNSIGNED"
        print(query)
        self.db1.execute(query)
        query = "ALTER TABLE `" + self.name + "` ADD FOREIGN KEY (" + parent_id + ") REFERENCES " + parent + "("+self.id_column_name+")"
        print(query)
        self.db1.execute(query)

    @save_migration
    def add_column(self, column_name, column_type):
        assert len(column_name) > 1
        command = "ALTER TABLE `" + self.name + "` ADD COLUMN `" + column_name + "` " + column_type
        try:
            self.db1.execute(command)
            self.columns.append(column_name)
            self.types.append(column_type)
        except Exception as e:
            print("Cant add column to table.")

    @save_migration
    def drop_column(self, column_name):
        assert len(column_name) > 1
        command = "ALTER TABLE `" + self.name + "` DROP COLUMN " + column_name
        try:
            print(command)
            self.db1.execute(command)
            index = self.columns.index(column_name)
            self.db1.columns.remove(column_name)
            self.db1.types.remove(self.db1.types[index])
        except Exception as e:
            print(e)
            print("Cant drop " + self.name)

    @save_migration
    def modify_column(self, column_name, new_column_type):
        assert len(column_name) > 1
        command = "ALTER TABLE `" + self.name + "` MODIFY COLUMN `" + column_name + "` " + new_column_type
        print(command)
        try:
            self.db1.execute(command)
            index = self.columns.index(column_name)
            self.types[index] = new_column_type
        except:
            print("Cant modify column to table.")


############### XLSX ##################

class XlsxTable(AbstractTable):
    def __init__(self, db1, name, columns=None, types=None, id_column_name = "id", number_of_retries=5):
        super().__init__(db1, name, columns, types)
        self.id_column_name = id_column_name
        self.NUMBER_OF_RETRIES = number_of_retries

        table_filename = f"{self.name}.csv" if self.db1.is_csv else f"{self.name}.xlsx"
        self.table_directory_path: pathlib.Path = self.db1.db_directory_path / table_filename

    def _save_table(self, df: pd.DataFrame):
        if self.db1.is_csv:
            df.to_csv(self.table_directory_path, index=False)
        else:
            df.to_excel(self.table_directory_path, index=False)

    def get_all_columns(self):
        df=self.select_to_df()
        columns=df.columns.tolist()
        return(columns)


    def get_all_types(self):
        df=self.select_to_df()
        dtypes=df.dtypes.tolist() 
        types = [str(x) for x in dtypes]
        clean_types=[x.replace("object","str") for x in types] #Todo support more types
        
        return(clean_types) 
  
    
    
    @classmethod
    def init_all_columns(cls, db1, name, id_column_name="id"):
        temporary_table = cls(db1, name, id_column_name)
        columns = temporary_table.get_all_columns()
        types = temporary_table.get_all_types()

        if temporary_table.id_column_name in columns:
            id_col_index = columns.index(temporary_table.id_column_name)
            columns.pop(id_col_index)
            columns.insert(0, temporary_table.id_column_name)
            types.pop(id_col_index)
            types.insert(0, "int")

        return (cls(db1, name, columns, types, id_column_name=id_column_name))    



    def create(self):
        if not self.table_directory_path.exists():
            df = pd.DataFrame(columns=self.columns)
            self._save_table(df)
        else:
            print(f"Table '{self.name}' already exists")

    def drop(self):
        self.table_directory_path.unlink(missing_ok=True)

    def select_to_df(self):
        # String must be typed with 'object' otherwise they get parsed arbitrarily and
        # then cast to string, e.g. 'true' -> True -> 'True'
        column_type_map = {
            column: object for column, type_ in self.column_type_dict.items() if type_ == "str"
        }
        date_columns = [
            column for column, type_ in self.column_type_dict.items() if type_ == "datetime"
        ]

        # BUG: If XlsxTable is being accessed by multiple threads, read operation
        # might fail due to race conditions. Add retry mechanism to handle these cases.
        for attempt in range(self.NUMBER_OF_RETRIES):
            try:
                df = self._select(column_type_map, date_columns)
            except Exception:
                # print(f"Error while reading data into XlsxTable: {e}")
                # df = pd.DataFrame(columns=self.columns)
                if attempt < self.NUMBER_OF_RETRIES - 1:
                    time.sleep(0.1)
                else:
                    print(f"Failed to read data from {self.table_directory_path}, returning empty DataFrame")
                    df = pd.DataFrame(columns=self.columns)
        return df

    def _select(self, column_type_map, date_columns):
        if self.db1.is_csv:
            df = pd.read_csv(
                self.table_directory_path, dtype=column_type_map, parse_dates=date_columns,
                encoding='utf-8'
            )
        else:
            df = pd.read_excel(
                self.table_directory_path, dtype=column_type_map, parse_dates=date_columns
            )
        df.replace({np.nan: None}, inplace=True)
        return df

    def insert_from_df(self, df, batch=1, try_mode=False, debug_mode=False, adjust_df=False, insert_id=False):
        
        if adjust_df:
            df = self._adjust_df(df, debug_mode)
        
        if insert_id:
            assert len(df.columns) == len(self.columns)
            assert set(df.columns) == set(self.columns)
            
            inserted_columns=self.columns
        else:
            assert len(df.columns) + 1 == len(self.columns)  # +1 because of id column

        original_df = self.select_to_df()

        # Need to save original dtypes, otherwise some of them will be changed during DF concatenation
        df_types = df.dtypes

        original_df.index = original_df[self.id_column_name]
        try:
            original_df = original_df.drop(original_df.columns[0], axis=1)
        except:
            print("First column could not be dropped")

        # handling nan values -> change to NULL TODO  
        for column in list(df.columns):
            df.loc[pd.isna(df[column]), column] = None

        def concat_with_reset_index_in_second_df(original_df, df):
            """Subsitute of reset_index method because we need to maintain the ids of original df"""
            if len(original_df.index) > 0:
                maximal_index = max(original_df.index)
            else:
                maximal_index = 0

            df.index = df.index + maximal_index + 1
            df.reindex(columns=original_df.columns.tolist())  # to ensure that columns get correctly inserted
            result_df = pd.concat([original_df, df]).astype(df_types)       # Cast to original dtypes

            return result_df

        df = concat_with_reset_index_in_second_df(original_df, df)
        df[self.id_column_name] = df.index
        df = df.reindex(columns=[self.id_column_name] + df.columns[:-1].tolist())  # uid as a first column
        df.reset_index(drop=True, inplace=True)

        self._save_table(df)
        self.db1.last_table_inserted_into = self.name

    def replace_from_df(self, df):
        assert len(df.columns) == len(self.columns)  # +1 because of id column
        #df.drop(df.columns[0], axis=1, inplace=True)
        self._save_table(df)

    # def update(self, variable_assign: str, where: Optional[str] = None):
    #     def split_assign(variable_assign):
    #         variable = variable_assign.split("=")[0]
    #         value = variable_assign.split("=")[1]
    #         try:
    #             value = int(value)  # integers
    #         except:
    #             value = value.split("'")[1]  # strings
    #         return (variable, value)

    #     variable, value = split_assign(variable_assign)
    #     df = self.select_to_df()
    #     print(where)
    #     print(variable, value)
    #     if where is None:
    #         df[variable] = value
    #         print(df)
    #     else:
    #         where_variable, where_value = split_assign(where)
    #         df[variable] = df[where_variable].apply(lambda x: value if str(x) == str(where_value) else x) #
    #     self.replace_from_df(df)

    def update(self, sql_column_update_string: str, sql_where_string: str) -> None:
        """Decompose provided parts of SQL UPDATE statement and update the xlsx file accordingly.

        TODO: Very fragile, unintuitive, and error-prone. Should be replaced with update_from_df().
        BUG: This will fail spectacularly on any values containing ',' in a
        `sql_column_update_string` or `sql_where_string'

        :param sql_column_update_string: e.g. "project_key='jakubatforloop.ai', project_name='Untitled Project'
        :type sql_column_update_string: str
        :param sql_where_string: e.g. "uid='1'"
        :type sql_where_string: str
        """

        # "project_key='jakubatforloop.ai', project_name='Untitled Project', last_active_pipeline_uid='1'"
        column_value_strings = sql_column_update_string.split(",")
        # ["project_key='jakubatforloop.ai'", " project_name='Untitled Project'", " last_active_pipeline_uid='1'"]
        column_value_pairs = [pair.split("=") for pair in column_value_strings]
        # [['project_key', "'jakubatforloop.ai'"], [' project_name', "'Untitled Project'"], [' last_active_pipeline_uid', "'1'"]]
        column_value_pairs = [
            [column.strip(' "\''), value.strip(' "\'')] for column, value in column_value_pairs
        ]
        # [['project_key', 'jakubatforloop.ai'], ['project_name', 'Untitled Project'], ['last_active_pipeline_uid', '1']]

        where_column, where_value = sql_where_string.split("=")
        where_value = where_value.strip(' "\'')

        df = self.select_to_df()
        columns_to_update = [pair[0] for pair in column_value_pairs]
        values_to_update = [pair[1] for pair in column_value_pairs]
        df.loc[df[where_column] == where_value, columns_to_update] = values_to_update

        self._save_table(df)

    def update_from_df(self, update_df: pd.DataFrame, where_column: str, where_value: Any) -> None:
        """Update the xlsx file with the provided dataframe.

        :param update_df: Dataframe with updated values - MUST only hold a single row
        :type update_df: pd.DataFrame
        :param where_column: Column name to use for WHERE clause.
        :type where_column: str
        :param where_value: Value to use for WHERE clause.
        :type where_value: Any
        """
        if not len(update_df) == 1:
            raise ValueError("There can only be one row in the UPDATE dataframe")

        table_df = self.select_to_df()
        table_df.loc[table_df[where_column] == where_value, update_df.columns] = update_df.values

        self._save_table(table_df)

    def delete(self, where=None) -> Optional[int]:
        def split_assign(variable_assign):
            variable = variable_assign.split("=")[0]
            value = variable_assign.split("=")[1]
            try:
                value = int(value)  # integers
            except:
                value = value.split("'")[1]  # strings
            return (variable, value)

        df = self.select_to_df()
        number_of_records = len(df)
        if where is None:
            df = df.iloc[0:0]
            deleted_count = number_of_records
        else:
            where_variable, where_value = split_assign(where)
            df.drop(df[df[where_variable] == where_value].index, inplace=True)
            deleted_count = number_of_records - len(df)
        self.replace_from_df(df)
        return deleted_count    

    def extract_last_id(self) -> Any:
        assert self.name == self.db1.last_table_inserted_into, "Last table inserted into is not the same as the table being queried"
        df = self.select_to_df()
        return df.iloc[-1][self.id_column_name]
