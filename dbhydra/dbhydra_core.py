"""DB Hydra ORM"""
import pymongo
import pyodbc
import pandas as pd
import numpy as np
import pymysql as MySQLdb
import psycopg2
import math
import json
import ast

from google.cloud import bigquery
from google.oauth2 import service_account


import abc
from contextlib import contextmanager

MONGO_OPERATOR_DICT ={"=": "$eq", ">": "$gt", ">=": "$gte", " IN " : "$in", "<": "$lt", "<=": "$lte", "<>": "$ne"}



def read_file(file):
    """Reads txt file -> list"""

    with open(file, "r") as f:
        rows = f.readlines()

        for i,row in enumerate(rows):
            rows[i]=row.replace("\n","")
    return(rows)
    
def read_connection_details(config_file):
    connection_details=read_file(config_file)
    db_details={}
    for detail in connection_details:
        key=detail.split("=")[0]
        value=detail.split("=")[1]
        db_details[key]=value

        # Skip empty lines to avoid error when reading config file
        if not detail:
            continue

    print(", ".join([db_details['DB_SERVER'],db_details['DB_DATABASE'],db_details['DB_USERNAME']]))
    return(db_details)



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
        migrator.migration_list_to_json()
        function(instance, *args, **kw)

    return (new_function)



class Migrator:
    def __init__(self, db=None):
        self.db = db
        self.migration_number = 1
        self.migration_list = []

    def process_migration_dict(self, migration_dict):
        assert len(migration_dict.keys()) == 1
        operation = list(migration_dict.keys())[0]
        options = migration_dict[operation]
        if operation == "create":
            if (isinstance(self.db, Mysqldb)):

                table = MysqlTable(self.db, options["table_name"], options["columns"], options["types"])
            elif (isinstance(self.db, PostgresDb)):
                table = PostgresTable(self.db, options["table_name"], options["columns"], options["types"])
            table.create()
        elif operation == "drop":
            if (isinstance(self.db, Mysqldb)):
                table = MysqlTable(self.db, options["table_name"])
            elif (isinstance(self.db, PostgresDb)):
                table = PostgresTable(self.db, options["table_name"])
            table.drop()
        elif operation == "add_column":
            if (isinstance(self.db, Mysqldb)):
                table = MysqlTable(self.db, options["table_name"])
            elif (isinstance(self.db, PostgresDb)):
                table = PostgresTable(self.db, options["table_name"])
            table.initialize_columns()
            table.initialize_types()
            table.add_column(options["column_name"], options["column_type"])
        elif operation == "modify_column":
            if (isinstance(self.db, Mysqldb)):
                table = MysqlTable(self.db, options["table_name"])
            elif (isinstance(self.db, PostgresDb)):
                table = PostgresTable(self.db, options["table_name"])
            table.initialize_columns()
            table.initialize_types()
            table.modify_column(options["column_name"], options["column_type"])
        elif operation == "drop_column":
            if (isinstance(self.db, Mysqldb)):
                table = MysqlTable(self.db, options["table_name"])
            elif (isinstance(self.db, PostgresDb)):
                table = PostgresTable(self.db, options["table_name"])
            table.initialize_columns()
            table.initialize_types()
            table.drop_column(options["column_name"])

    def next_migration(self):
        self.migration_number += 1
        self.migration_list = []

    def migrate(self, migration_list):
        for i, migration_dict in enumerate(migration_list):
            self.process_migration_dict(migration_dict)

    def migrate_from_json(self, filename):
        with open(filename, "r") as f:
            rows = f.readlines()[0].replace("\n", "")
        result = json.loads(rows)
        for dict in result:
            self.process_migration_dict(dict)
        return (result)

    def migration_list_to_json(self, filename=None):
        result = json.dumps(self.migration_list)


        if filename is None or filename == "" or filename.isspace():
            with open("migrations/migration-" + str(self.migration_number) + ".json", "w+") as f:
                f.write(result)
        else:
            with open(f"{filename}.json", "w+") as f:
                f.write(result)

    def create_migrations_from_df(self, name, dataframe):


        columns, return_types = self.extract_columns_and_types_from_df(dataframe)

        migration_dict = {"create": {"table_name": name, "columns": columns, "types": return_types}}
        self.migration_list.append(migration_dict)
        self.migration_list_to_json()
        # return columns, return_types

    def extract_columns_and_types_from_df(self, dataframe):
        columns = list(dataframe.columns)

        return_types = []
        for column in dataframe:
            t = dataframe.loc[0, column]
            try:
                return_types.append(type(t.item()).__name__)
            except:
                # length = 2**( int(dataframe[col].str.len().max()) - 1).bit_length()
                length = int(dataframe[column].str.len().max())
                length += 0.1 * length
                length = int(math.ceil(length / 10.0)) * 10
                return_types.append(f'nvarchar({length})' if type(t).__name__ == 'str' else type(t).__name__)

        if (columns[0] != "id"):
            columns.insert(0, "id")
            return_types.insert(0, "int")

        return columns, return_types


class AbstractDB(abc.ABC):
    def __init__(self, config_file="config.ini", db_details=None):
        if db_details is None:
            db_details = read_connection_details(config_file)
            
        self.locally=True
        if db_details["LOCALLY"]=="False":
            self.locally=False  
        
        self.DB_SERVER = db_details["DB_SERVER"]
        self.DB_DATABASE = db_details["DB_DATABASE"]
        self.DB_USERNAME = db_details["DB_USERNAME"]
        self.DB_PASSWORD = db_details["DB_PASSWORD"]

        if "DB_PORT" in db_details.keys():
            self.DB_PORT = int(db_details["DB_PORT"])
        else:
            self.DB_PORT = None

        if "DB_DRIVER" in db_details.keys():
            self.DB_DRIVER = db_details["DB_DRIVER"]
        else:
            self.DB_DRIVER = "ODBC Driver 13 for SQL Server"

        self.connect_to_db()


    @abc.abstractmethod
    def connect_locally(self):
        pass

    @abc.abstractmethod
    def connect_remotely(self):
        pass


    def _connect(self):
        if self.locally:
            self.connect_locally()
        else:
            self.connect_remotely()


    def connect(self):
        print("DEPRECATION WARNING: use `connect_to_db` context manager instead of `connect` method")
        if self.locally:
            self.connect_locally()
        else:
            self.connect_remotely()

    @contextmanager
    def connect_to_db(self):
        try:
            self._connect()
            yield None
        finally:
            self.close_connection()

    def execute(self, query):
        self.cursor.execute(query)
        self.cursor.commit()

    def close_connection(self):
        self.connection.close()
        print("DB connection closed")

    def initialize_migrator(self):
        self.migrator = Migrator(self)


class AbstractDBPostgres(AbstractDB):
    # TODO: Config_mongo.ini in postgres?
    def __init__(self, config_file="config-mongo.ini", db_details=None):
        super().__init__(config_file, db_details)

    def close_connection(self):
        self.cursor.close()
        print("DB connection closed")


class AbstractDBMongo(AbstractDB):
    def __init__(self, config_file="config-mongo.ini", db_details=None):
        super().__init__(config_file, db_details)

    # TODO, execute, close


# AWFUL NAME, SHOULD BE RENAMED WITH MAJOR RELEASE (This connects basic SQL i suppose)
class db(AbstractDB):
    def connect_remotely(self):

        self.connection = pyodbc.connect(
            r'DRIVER={' + self.DB_DRIVER + '};'
            r'SERVER=' + self.DB_SERVER + ';'
            r'DATABASE=' + self.DB_DATABASE + ';'
            r'UID=' + self.DB_USERNAME + ';'
            r'PWD=' + self.DB_PASSWORD + '',
            timeout=1

        )
        self.cursor = self.connection.cursor()
        print("DB connection established")

    def connect_locally(self):
        self.connection = pyodbc.connect(
            r'DRIVER={' + self.DB_DRIVER + '};'
            r'SERVER=' + self.DB_SERVER + ';'
            r'DATABASE=' + self.DB_DATABASE + ';'
            r'TRUSTED_CONNECTION=yes;',
            timeout=1
            # r'PWD=' + self.DB_PASSWORD + '')
        )

        self.cursor = self.connection.cursor()
        print("DB connection established")

    def get_all_tables(self):
        sysobjects_table = Table(self, "sysobjects", ["name"], ["nvarchar(100)"])
        query = "select name from sysobjects where xtype='U'"
        rows = sysobjects_table.select(query)
        return (rows)

    def generate_table_dict(self):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = Table.init_all_columns(self, table)

        return (table_dict)

    def get_foreign_keys_columns(self):
        sys_foreign_keys_columns_table = Table(self, "sys.foreign_key_columns",
                                               ["parent_object_id", "parent_column_id", "referenced_object_id",
                                                "referenced_column_id"], ["int", "int", "int", "int"])
        query = "select parent_object_id,parent_column_id,referenced_object_id,referenced_column_id from sys.foreign_key_columns"
        rows = sys_foreign_keys_columns_table.select(query)

        sys_foreign_keys_columns_table = Table(self, "sys.tables", ["object_id", "name"], ["int", "nvarchar(100)"])
        query = "select object_id,name from sys.tables"
        table_names = sys_foreign_keys_columns_table.select(query)

        table_id_name_dict = {x[0]: x[1] for x in table_names}

        foreign_keys = []
        for i, row in enumerate(rows):
            fk = {"parent_table": table_id_name_dict[row[0]], "parent_column_id": row[1] - 1,
                  "referenced_table": table_id_name_dict[row[2]],
                  "referenced_column_id": row[3] - 1}  # minus 1 because of indexing from 0
            foreign_keys.append(fk)

        return (foreign_keys)



class Mysqldb(AbstractDB):
    def connect_locally(self):
        self.connection = MySQLdb.connect(host=self.DB_SERVER, user=self.DB_USERNAME, password=self.DB_PASSWORD,
                                          database=self.DB_DATABASE)
        self.cursor = self.connection.cursor()
        print("DB connection established")

    def connect_remotely(self):
        if self.DB_PORT is not None:
            self.connection = MySQLdb.connect(host=self.DB_SERVER, port=self.DB_PORT, user=self.DB_USERNAME,
                                              password=self.DB_PASSWORD, database=self.DB_DATABASE)
        else:
            self.connection = MySQLdb.connect(host=self.DB_SERVER, user=self.DB_USERNAME, password=self.DB_PASSWORD,
                                              database=self.DB_DATABASE)
        self.cursor = self.connection.cursor()
        print("DB connection established")

    def create_new_db(self):
        self.connection = MySQLdb.connect(host=self.DB_SERVER, user=self.DB_USERNAME,
                                             password=self.DB_PASSWORD,
                                             charset="utf8mb4" , cursorclass=MySQLdb.cursors.DictCursor)

        self.cursor = self.connection.cursor()

        create_db_command = "CREATE DATABASE " + self.DB_DATABASE
        self.execute(create_db_command)

    def execute(self, query):

        self.cursor.execute(query)
        self.connection.commit()

    def get_all_tables(self):
        sysobjects_table = Table(self, "information_schema.tables", ["TABLE_NAME"], ["nvarchar(100)"])
        query = "SELECT TABLE_NAME,TABLE_TYPE,TABLE_SCHEMA FROM information_schema.tables where TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='" + self.DB_DATABASE + "' ;"
        rows = sysobjects_table.select(query)
        tables = [x[0] for x in rows]
        return (tables)

    def generate_table_dict(self):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = MysqlTable.init_all_columns(self, table)
        return (table_dict)


class PostgresDb(AbstractDBPostgres):

    def connect_locally(self):
        self.connection= psycopg2.connect(
            host=self.DB_SERVER,
            database=self.DB_DATABASE,
            user=self.DB_USERNAME,
            password=self.DB_PASSWORD)
        self.cursor=self.connection.cursor()

    def connect_remotely(self):
        self.connection = psycopg2.connect(
            host=self.DB_SERVER,
            database=self.DB_DATABASE,
            user=self.DB_USERNAME,
            password=self.DB_PASSWORD)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()

    def execute(self, query):
        self.cursor.execute(query)
        self.connection.commit()

        # return  [''.join(i) for i in self.cursor.fetchall()]

    def get_all_tables(self):
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        return [''.join(i) for i in self.cursor.fetchall()]

    def generate_table_dict(self):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = PostgresTable.init_all_columns(self, table)

        return (table_dict)





class BigQueryDb:
    def __init__(self,credentials_path,project_id):
        #super().__init__(None,None)
        self.credentials_path=credentials_path
        self.project_id=project_id

        self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],)

        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id,)



    def connect_remotely(self):
        print("Connect remotely")

    def connect_locally(self):
        print("Connect locally")

    def close_connection(self):
        self.client.close()

    def select_to_df(self, query):
        print(query)
        df = pd.read_gbq(query=query, project_id=self.project_id, credentials=self.credentials)

        return df

    def execute(self,query):


        # query_job = self.client.query(
        #     """
        #     SELECT
        #       CONCAT(
        #         'https://stackoverflow.com/questions/',
        #         CAST(id as STRING)) as url,
        #       view_count
        #     FROM `bigquery-public-data.stackoverflow.posts_questions`
        #     WHERE tags like '%google-bigquery%'
        #     ORDER BY view_count DESC
        #     LIMIT 10"""
        # )



        query_job = self.client.query(query)


        results = query_job.result()  # Waits for job to complete.

        for row in results:
            print(row)

            #print("{} : {} views".format(row.id, row.link,row.title))



class MongoDb(AbstractDBMongo):

    def connect_locally(self):
        self.connection= pymongo.MongoClient(
            host=self.DB_SERVER,
            authSource=self.DB_DATABASE,
            username=self.DB_USERNAME,
            password=self.DB_PASSWORD,
            authMechanism='SCRAM-SHA-256'
            )

        self.database=self.connection.get_database(self.DB_DATABASE)
        print('Connected locally')

    def connect_remotely(self):
        self.connection = pymongo.MongoClient(
            "mongodb+srv://" + self.DB_USERNAME + ":" + self.DB_PASSWORD + "@" + self.DB_SERVER + "/" + self.DB_DATABASE + "?retryWrites=true&w=majority")
        print(self.connection.list_database_names())
        self.database = self.connection[self.DB_DATABASE]
        print(self.database)
        print("DB connection established")

    def execute(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def get_all_tables(self):
        return self.database.list_collection_names()

    def createTable(self, name):
        return self.database[name]

    def close_connection(self):
        self.connection.close()
        print("DB connection closed.")

    def generate_table_dict(self):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = MongoTable.init_all_columns(self, table)

        return (table_dict)



# Tables
class AbstractSelectable:
    def __init__(self, db1, name, columns=None):
        self.db1 = db1
        self.name = name
        self.columns = columns

    def select(self, query):

        """given SELECT query returns Python list"""
        """Columns give the number of selected columns"""
        print(query)
        self.db1.cursor.execute(query)
        column_string = query.lower().split("from")[0]
        if "*" in column_string:
            columns = len(self.columns)
        elif column_string.find(",") == -1:
            columns = 1
        else:
            columns = len(column_string.split(","))
        rows = self.db1.cursor.fetchall()
        print(rows)
        if columns == 1:
            cleared_rows_list = [item[0] for item in rows]

        if columns > 1:
            cleared_rows_list = []
            for row in rows:  # Because of unhashable type: 'pyodbc.Row'
                list1 = []

                for i in range(columns):
                    # print(row)
                    list1.append(row[i])
                cleared_rows_list.append(list1)
        return (cleared_rows_list)


    def select_all(self):
        list1 = self.select("SELECT * FROM " + self.name)
        return (list1)

    def select_to_df(self):
        rows = self.select_all()
        table_columns = self.columns
        demands_df = pd.DataFrame(rows, columns=table_columns)
        return (demands_df)

    def export_to_xlsx(self):
        list1 = self.select_all()
        df1 = pd.DataFrame(list1, columns=["id"] + self.columns)
        df1.to_excel("items.xlsx")


class Selectable(AbstractSelectable):  # Tables, views, and results of joins
    pass


class MysqlSelectable(AbstractSelectable):
    def select(self, query):

        """TODO"""
        print(query)
        self.db1.execute(query)
        rows = self.db1.cursor.fetchall()
        return (rows)


class AbstractJoinable(AbstractSelectable):
    def __init__(self, db1, name, columns=None):
        super().__init__(db1, name, columns)

    def inner_join(self, joinable, column1, column2):
        join_name = self.name + " INNER JOIN " + joinable.name + " ON " + column1 + "=" + column2
        join_columns = list(set(self.columns) | set(joinable.columns))
        new_joinable = Joinable(self.db1, join_name, join_columns)
        return (new_joinable)



class Joinable(Selectable):
    pass


class AbstractTable(AbstractJoinable):
    def __init__(self, db1, name, columns=None, types=None):
        super().__init__(db1, name, columns)
        self.types = types

    # @save_migration

    def drop(self):
        query = "DROP TABLE " + self.name
        print(query)
        self.db1.execute(query)

    def update(self, variable_assign, where=None):
        if where is None:
            query = "UPDATE " + self.name + " SET " + variable_assign
        else:
            query = "UPDATE " + self.name + " SET " + variable_assign + " WHERE " + where
        print(query)
        self.db1.execute(query)

    def _adjust_df(self, df: pd.DataFrame, debug_mode=False) -> pd.DataFrame:
        """
        Adjust DataFrame in case number of its columns differs from number of columns in DB table.
        :param df: original DF
        :param debug_mode: in debug mode a warning will be printed to console
        :return: adjusted DF
        """

        df_copy = df.copy()

        difference_columns_missing = list(set(self.columns).difference(set(df_copy.columns)).difference({'id'}))

        if difference_columns_missing:
            if debug_mode:
                print(f'DataFrame missing columns {difference_columns_missing} will be added as empty.')

            for col in difference_columns_missing:
                df_copy[col] = np.nan

        difference_columns_extra = list(set(df_copy.columns).difference(set(self.columns)))

        if difference_columns_extra:
            if debug_mode:
                print(f'DataFrame extra columns {difference_columns_extra} will be removed.')

            df_copy.drop(difference_columns_extra, axis=1, inplace=True)

        # Ensure the same order of columns in DF as in DB table
        cols_order = [x for x in self.columns if x != 'id']
        df_copy = df_copy[cols_order]

        return df_copy


    def insert_from_df(self,df,batch=1,try_mode=False, debug_mode=False, adjust_df=False):

        if adjust_df:
            df = self._adjust_df(df, debug_mode)

        # assert len(df.columns)+1==len(self.columns) #+1 because of id column


        pd_nullable_dtypes = {pd.Int8Dtype(), pd.Int16Dtype(), pd.Int32Dtype(), pd.Int64Dtype(),
                              pd.UInt8Dtype(), pd.UInt16Dtype(), pd.UInt32Dtype(), pd.UInt64Dtype(),
                              pd.Float32Dtype(), pd.Float64Dtype()}
        pd_nullable_columns = df.dtypes.isin(pd_nullable_dtypes)

        # cast pd nullable columns to float - changing to 'NULL' then proceeds without an error.
        # in database floats are again casted to integers where required
        df = df.copy()
        df.loc[:, pd_nullable_columns] = df.loc[:, pd_nullable_columns].astype(float)

        # TODO: handling nan values -> change to NULL
        for column in list(df.columns):
            df.loc[pd.isna(df[column]), column] = "NULL"

        rows=df.values.tolist()
        for i,row in enumerate(rows):
            for j,record in enumerate(row):
                if type(record)==str:
                    rows[i][j]="'"+record+"'"
        self.insert(rows,batch=batch,try_mode=try_mode, debug_mode=False)

    def delete(self, where=None):

        if where is None:
            query = "DELETE FROM " + self.name
        else:
            query = "DELETE FROM " + self.name + " WHERE " + where
        print(query)
        self.db1.execute(query)


class PostgresTable(AbstractTable):
    def __init__(self, db1, name, columns=None, types=None):
        super().__init__(db1, name, columns)
        self.types = types
        print("==========================================")

    def get_all_columns(self):
        information_schema_table = Table(self.db1, 'INFORMATION_SCHEMA.COLUMNS')
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        columns = information_schema_table.select(query)
        print(columns)
        return (columns)

    def get_all_types(self):
        information_schema_table = Table(self.db1, 'INFORMATION_SCHEMA.COLUMNS', ['DATA_TYPE'], ['nvarchar(50)'])
        query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        types = information_schema_table.select(query)
        print(types)
        return (types)

    def select_all(self):
        print(super().select_all())
        return [i for i in super().select_all()]

    @classmethod
    def init_all_columns(cls, db1, name):
        temporary_table = cls(db1, name)
        columns = temporary_table.get_all_columns()
        types = temporary_table.get_all_types()
        return (cls(db1, name, columns, types))

    @save_migration
    def create(self, foreign_keys=None):
        assert len(self.columns) == len(self.types)
        assert self.columns[0] == "id"
        assert self.types[0] == "int"
        query = "CREATE TABLE " + self.name + "(id SERIAL PRIMARY KEY,"
        for i in range(1, len(self.columns)):
            query += self.columns[i] + " " + self.types[i] + ","

        query = query[:-1]
        query += ")"
        print(query)
        try:
            self.db1.cursor.execute(query)
        except Exception as e:
            print("Table " + self.name + " already exists:", e)
            print("Check the specification of table columns and their types")


    def insert(self,rows,batch=1,replace_apostrophes=True,try_mode=False, debug_mode=False):
        print("INSERTING!!!")
        assert len(self.columns) == len(self.types)
        print(self.types)
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
                if rows[k][j] == "NULL" or rows[k][j] == "'NULL'" or rows[k][j] == None or rows[k][j] == "None":  # NaN hodnoty
                    if "int" in self.types[j + 1]:

                        if replace_apostrophes:
                            rows[k][j] = str(rows[k][j]).replace("'", "")
                        query += "NULL,"
                    else:
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
                elif "datetime" in self.types[j + 1]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "'" + str(rows[k][j]) + "',"
                elif "date" in self.types[j + 1]:
                    query += "'" + str(rows[k][j]) + "',"



                else:
                    query += str(rows[k][j]) + ","

            query=query[:-1]+"),"
            if k%batch==batch-1 or k==len(rows)-1:
                query=query[:-1]

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
                                file.write("Query "+str(query)+" could not be inserted:"+str(e)+"\n")



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


class MongoTable():
    def __init__(self, db, name, columns=[], types=[]):
        self.name = name
        self.db1 = db
        print("==========================================")
        print(type(db))
        print(db)
        self.collection = self.db1.createTable(name)


    def drop(self):
        return self.collection.drop()

    def update_collection(self):
        self.collection = self.db1.createTable(self.name)

    def insert(self, document):
        return self.collection.insert_one(document)

    def insertMore(self, documents):
        return self.collection.insert_many(documents)

    def select(self, query, columns={}):

        if (len(columns) == 0):

            return list(self.collection.find(query))
        else:
            return list(self.collection.find(query, columns))

    def select_all(self, query={}):
        return list(self.collection.find(query))

    def selectSort(self, query, fieldname, direction, columns={}):
        if (len(columns) == 0):
            return list(self.collection.find(query).sort(fieldname, direction))
        else:
            return list(self.collection.find(query, columns).sort(fieldname, direction))

    def delete(self, query={}):
        self.collection = self.db1.createTable(self.name)
        return self.collection.delete_many(query)

    def update(self, query, newvalues):
        return self.collection.update_many(query, newvalues)


    def insertFromDataFrame(self, dataframe):
        dict_from_df = dataframe.to_dict('record')
        return self.collection.insert_many(dict_from_df)

    def select_to_df(self, query={}):
        print(type(pd.DataFrame(list(self.collection.find(query)))))
        print(pd.DataFrame(list(self.collection.find(query))))
        return pd.DataFrame(list(self.collection.find(query)))

    @classmethod
    def init_all_columns(cls, db1, name):

        temporary_table = cls(db1, name)
        values = temporary_table.get_columns_types()
        columns = values[0]
        types = values[1]
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


class Table(Joinable, AbstractTable):
    def __init__(self, db1, name, columns=None, types=None):
        """Override joinable init"""
        super().__init__(db1, name, columns)
        self.types = types

    @classmethod
    def init_all_columns(cls, db1, name):
        temporary_table = cls(db1, name)
        columns = temporary_table.get_all_columns()
        types = temporary_table.get_all_types()
        return (cls(db1, name, columns, types))


    def get_all_columns(self):
        information_schema_table = Table(self.db1, 'INFORMATION_SCHEMA.COLUMNS')
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        columns = information_schema_table.select(query)
        return (columns)

    def get_all_types(self):
        information_schema_table = Table(self.db1, 'INFORMATION_SCHEMA.COLUMNS', ['DATA_TYPE'], ['nvarchar(50)'])
        query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        types = information_schema_table.select(query)
        return (types)

    def create(self):
        assert len(self.columns) == len(self.types)
        assert self.columns[0] == "id"
        assert self.types[0] == "int"
        query = "CREATE TABLE " + self.name + "(id INT IDENTITY(1,1) NOT NULL,"
        for i in range(1, len(self.columns)):
            query += self.columns[i] + " " + self.types[i] + ","
        query += "PRIMARY KEY(id))"

        print(query)
        try:
            self.db1.execute(query)
        except Exception as e:
            print("Table " + self.name + " already exists:", e)
            print("Check the specification of table columns and their types")

    def insert(self,rows,batch=1,replace_apostrophes=True,try_mode=False, debug_mode=False):

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

            query=query[:-1]+"),"
            if k%batch==batch-1 or k==len(rows)-1:
                query=query[:-1]

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





    def get_foreign_keys_for_table(self,table_dict,foreign_keys):
        #table_dict is in format from db function: generate_table_dict()
        #foreign_keys are in format from db function: get_foreign_keys_columns()
        parent_foreign_keys=[]
        for i,fk in enumerate(foreign_keys):
            if fk["parent_table"]==self.name:

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


class MysqlTable(MysqlSelectable, AbstractTable):
    def __init__(self, db1, name, columns=None, types=None):
        super().__init__(db1, name, columns)
        self.types = types


    def initialize_columns(self):
        information_schema_table = Table(self.db1, 'INFORMATION_SCHEMA.COLUMNS')
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "';"
        columns = information_schema_table.select(query)
        self.columns = columns

    def initialize_types(self):
        information_schema_table = Table(self.db1, 'INFORMATION_SCHEMA.COLUMNS', ['DATA_TYPE'], ['nvarchar(50)'])
        query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        types = information_schema_table.select(query)
        self.types = types

    def get_all_columns(self):
        information_schema_table = Table(self.db1, 'INFORMATION_SCHEMA.COLUMNS')
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '" + self.name + "'"
        columns = information_schema_table.select(query)

        return (columns)

    def get_all_types(self):

        information_schema_table=Table(self.db1,'INFORMATION_SCHEMA.COLUMNS',['DATA_TYPE'],['nvarchar(50)'])
        query="SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '"+self.name+"'"
        types=information_schema_table.select(query)
        return(types)


    @classmethod
    def init_all_columns(cls,db1,name):
        temporary_table=cls(db1,name)
        columns=temporary_table.get_all_columns()
        types=temporary_table.get_all_types()
        return(cls(db1,name,columns,types))

    def get_last_id(self):
        """
        Returns the biggest id from table
        """

        last_id = self.select(f"SELECT id FROM {self.name} ORDER BY id DESC LIMIT 1;")

        return last_id[0][0]

    def drop(self):
        query = "DROP TABLE " + self.name + ";"
        print(query)
        self.db1.execute(query)

    # @save_migration #TODO: Uncomment
    def create(self, foreign_keys=None):
        assert len(self.columns) == len(self.types)
        assert self.columns[0] == "id"
        assert self.types[0] == "int"
        query = "CREATE TABLE " + self.name + "(id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
        for i in range(1, len(self.columns)):
            query += self.columns[i] + " " + self.types[i] + ","

        query = query[:-1]
        query += ")"

        print(query)
        try:
            self.db1.execute(query)
        except Exception as e:
            print("Table " + self.name + " already exists:", e)
            print("Check the specification of table columns and their types")

    def insert(self,rows,batch=1,replace_apostrophes=True,try_mode=False, debug_mode=False):
        print("INSERTING!!!")
        assert len(self.columns) == len(self.types)
        print(self.types)
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
                if rows[k][j] == "NULL" or rows[k][j] == "'NULL'" or rows[k][j] == None or rows[k][j] == "None":  # NaN hodnoty
                    if "int" in self.types[j + 1]:

                        if replace_apostrophes:
                            rows[k][j] = str(rows[k][j]).replace("'", "")
                        query += "NULL,"
                    else:
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
                elif "datetime" in self.types[j + 1]:
                    if replace_apostrophes:
                        rows[k][j] = str(rows[k][j]).replace("'", "")
                    query += "'" + str(rows[k][j]) + "',"
                elif "date" in self.types[j + 1]:
                    query += "'" + str(rows[k][j]) + "',"



                else:
                    query += str(rows[k][j]) + ","

            query=query[:-1]+"),"
            if k%batch==batch-1 or k==len(rows)-1:
                query=query[:-1]

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
                                file.write("Query "+str(query)+" could not be inserted:"+str(e)+"\n")

    def add_foreign_key(self,foreign_key):
        parent_id=foreign_key['parent_id']
        parent=foreign_key['parent']
        query="ALTER TABLE "+self.name+" MODIFY "+parent_id+" INT UNSIGNED"
        print(query)
        self.db1.execute(query)
        query = "ALTER TABLE " + self.name + " ADD FOREIGN KEY (" + parent_id + ") REFERENCES " + parent + "(id)"
        print(query)
        self.db1.execute(query)

    @save_migration
    def add_column(self, column_name, column_type):
        assert len(column_name) > 1
        command = "ALTER TABLE " + self.name + " ADD COLUMN " + column_name + " " + column_type
        try:
            self.db1.execute(command)
            self.columns.append(column_name)
            self.types.append(column_type)
        except Exception as e:
            print("Cant add column to table.")

    @save_migration
    def drop_column(self, column_name):
        assert len(column_name) > 1
        command = "ALTER TABLE " + self.name + " DROP COLUMN " + column_name
        try:
            print(command)
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
        command = "ALTER TABLE " + self.name + " MODIFY COLUMN " + column_name + " " + new_column_type
        print(command)
        try:
            self.db1.execute(command)
            index = self.columns.index(column_name)
            self.types[index] = new_column_type
        except:
            print("Cant modify column to table.")


class XlsxDB:
    def __init__(self, name="new_db", config_file="config.ini"):
        self.name = name


        """
        db_details=read_connection_details(config_file)
        locally=True
        if db_details["LOCALLY"]=="False":
            locally=False
            
        if locally:
            self.DB_SERVER=db_details["DB_SERVER"]
            self.DB_DATABASE=db_details["DB_DATABASE"]
            self.DB_USERNAME = db_details["DB_USERNAME"]
            self.DB_PASSWORD = db_details["DB_PASSWORD"]
            self.connect_locally()
        else:
            self.DB_SERVER = db_details["DB_SERVER"]
            self.DB_DATABASE = db_details["DB_DATABASE"]
            self.DB_USERNAME = db_details["DB_USERNAME"]
            self.DB_PASSWORD = db_details["DB_PASSWORD"]
            self.connect_remotely()
        """

    def execute(self, query):
        pass
        # self.cursor.execute(query)
        # self.cursor.commit()

    def close_connection(self):
        pass
        # self.connection.close()
        # print("DB connection closed")


    def create_database(self):
        import os
        try:
            os.mkdir(self.name)
            print("Database created")
        except:
            print("Database is already created")


class XlsxTable(AbstractTable):
    def __init__(self, db1, name, columns=None, types=None):
        super().__init__(db1, name, columns)
        self.types = types

    def select_to_df(self):
        try:
            df = pd.read_excel(self.db1.name + "//" + self.name + ".xlsx")
            # cols=df.columns
            # print(cols)
            # df.set_index(cols[0],inplace=True)
            # df.drop(df.columns[0],axis=1,inplace=True)

        except Exception as e:
            print("Error: ", e)
            df = pd.DataFrame(columns=self.columns)
        return (df)


    def insert_from_df(self,df,batch=1,try_mode=False, debug_mode=False):
        assert len(df.columns)+1==len(self.columns) #+1 because of id column

        original_df = self.select_to_df()

        try:
            original_df = original_df.drop(original_df.columns[0], axis=1)
        except:
            print("First column could not be dropped")

        df.columns = original_df.columns
        # handling nan values -> change to NULL TODO
        for column in list(df.columns):
            df.loc[pd.isna(df[column]), column] = "NULL"

        def concat_with_reset_index_in_second_df(original_df, df):
            """Subsitute of reset_index method because we need to maintain the ids of original df"""
            maximal_index = max(original_df.index)
            df.index = df.index + maximal_index + 1
            original_df = original_df.append(df)
            # df=pd.concat([original_df,df])
            return (original_df)

        if len(original_df) > 0:
            df = concat_with_reset_index_in_second_df(original_df, df)

        df.to_excel(self.db1.name + "\\" + self.name + ".xlsx")

    def replace_from_df(self, df):
        assert len(df.columns) == len(self.columns)  # +1 because of id column
        df.drop(df.columns[0], axis=1, inplace=True)
        df.to_excel(self.db1.name + "\\" + self.name + ".xlsx")

    def update(self, variable_assign, where=None):
        def split_assign(variable_assign):
            variable = variable_assign.split("=")[0]
            value = variable_assign.split("=")[1]
            try:
                value = int(value)  # integers
            except:
                value = value.split("'")[1]  # strings
            return (variable, value)

        variable, value = split_assign(variable_assign)
        df = self.select_to_df()
        print(where)
        print(variable, value)
        if where is None:
            df[variable] = value
            print(df)
        else:
            where_variable, where_value = split_assign(where)
            df[variable] = df[where_variable].apply(lambda x: value if x == where_value else x)
        self.replace_from_df(df)

    def delete(self, where=None):
        def split_assign(variable_assign):
            variable = variable_assign.split("=")[0]
            value = variable_assign.split("=")[1]
            try:
                value = int(value)  # integers
            except:
                value = value.split("'")[1]  # strings
            return (variable, value)

        df = self.select_to_df()
        if where is None:
            df = df.iloc[0:0]
            print(df)
        else:
            where_variable, where_value = split_assign(where)
            df.drop(df[df[where_variable] == where_value].index, inplace=True)
        self.replace_from_df(df)


# dataframe - dictionary auxiliary functions
def df_to_dict(df, column1, column2):
    dictionary = df.set_index(column1).to_dict()[column2]
    return (dictionary)


def dict_to_df(dictionary, column1, column2):
    df = pd.DataFrame(list(dictionary.items()), columns=[column1, column2])
    return (df)
