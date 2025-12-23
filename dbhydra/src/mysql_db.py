

import datetime
import pymysql
from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import MysqlTable



class MysqlDb(AbstractDb):
    matching_table_class = MysqlTable
    python_database_type_mapping = PYTHON_TO_MYSQL_DATA_MAPPING = \
    {
    'int': "int",
    'float': "double",
    'str': "nvarchar(2047)",
    'tuple': "json",
    'list': "nvarchar(2047)",
    'dict': "nvarchar(2047)",
    'bool': "tinyint",
    'datetime': "datetime",
    'Jsonable': "json",
    'Blob': "mediumblob",
    'LongText': "longtext",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.identifier_quote = '`'

    def connect_locally(self):
        self.connection = pymysql.connect(host=self.DB_SERVER, user=self.DB_USERNAME, password=self.DB_PASSWORD,
                                          database=self.DB_DATABASE)
        self.cursor = self.connection.cursor()
        print("DB connection established")

    def connect_remotely(self):
        if self.DB_PORT is not None:
            self.connection = pymysql.connect(host=self.DB_SERVER, port=self.DB_PORT, user=self.DB_USERNAME,
                                              password=self.DB_PASSWORD, database=self.DB_DATABASE)
        else:
            self.connection = pymysql.connect(host=self.DB_SERVER, user=self.DB_USERNAME, password=self.DB_PASSWORD,
                                              database=self.DB_DATABASE)
        self.cursor = self.connection.cursor()
        print("DB connection established")

    def create_new_db(self):
        self.connection = pymysql.connect(host=self.DB_SERVER, port=self.DB_PORT, user=self.DB_USERNAME, 
                                          charset="utf8mb4", password=self.DB_PASSWORD)
        self.cursor = self.connection.cursor()
        create_db_command = "CREATE DATABASE " + self.DB_DATABASE
        self.execute(create_db_command)

        
    def execute(self, query, is_autocommitting=True):
        result=self.cursor.execute(query)
        if is_autocommitting:
            self.connection.commit()
            if self.debug_mode:
                with open("dbhydra_logs.txt","a+") as file:
                    file.write(str(datetime.datetime.now())+": DB "+str(self)+": execute() called, debug msg:"+str(self.debug_message)+"\n")
                with open("dbhydra_queries_logs.txt","a+") as file:
                    file.write(str(datetime.datetime.now())+": "+str(query)+"\n")
        return result
    

    def get_all_tables(self):
        sysobjects_table = MysqlTable(self, "information_schema.tables", ["TABLE_NAME"], ["nvarchar(100)"])
        query = "SELECT TABLE_NAME,TABLE_TYPE,TABLE_SCHEMA FROM information_schema.tables where TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='" + self.DB_DATABASE + "' ;"
        rows = sysobjects_table.select(query)
        tables = [x[0] for x in rows]
        return (tables)

    def generate_table_dict(self, id_column_name = "id"):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = MysqlTable.init_all_columns(self, table)
        return (table_dict)



class Mysqldb(MysqlDb):
    """Deprecated - do not remove until dbhydra 3.x"""
    def __init__(self, config_file="config.ini", db_details=None):
        print("Deprecation warning!, Mysqldb was renamed to MysqlDb and the old name will deprecated in future!")
        super().__init__(config_file=config_file, db_details=db_details)