
import sys

from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import SqlServerTable
from dbhydra.src.errors.exceptions import DbHydraException


#! Disabled on macOS --> problematic import
if sys.platform != "darwin":
    import pyodbc



class SqlServerDb(AbstractDb):
    matching_table_class = SqlServerTable
    
    def connect_remotely(self):
        if sys.platform == "darwin":
            raise DbHydraException("pyodbc library (MSSQL) not supported on macOS.")

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
        if sys.platform == "darwin":
            raise DbHydraException("pyodbc library (MSSQL) not supported on macOS.")
        
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
        sysobjects_table = SqlServerTable(self, "sysobjects", ["name"], ["nvarchar(100)"])
        query = "select name from sysobjects where xtype='U'"
        rows = sysobjects_table.select(query)
        return (rows)

    def generate_table_dict(self):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = SqlServerTable.init_all_columns(self, table)

        return (table_dict)

    def get_foreign_keys_columns(self):
        sys_foreign_keys_columns_table = SqlServerTable(self, "sys.foreign_key_columns",
                                               ["parent_object_id", "parent_column_id", "referenced_object_id",
                                                "referenced_column_id"], ["int", "int", "int", "int"])
        query = "select parent_object_id,parent_column_id,referenced_object_id,referenced_column_id from sys.foreign_key_columns"
        rows = sys_foreign_keys_columns_table.select(query)

        sys_foreign_keys_columns_table = SqlServerTable(self, "sys.tables", ["object_id", "name"], ["int", "nvarchar(100)"])
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



class db(SqlServerDb):
    """Deprecated - do not remove until dbhydra 3.x"""
    def __init__(self, config_file="config.ini", db_details=None):
        print("Deprecation warning!, db was renamed to SQLServerDb and the old name will deprecated in future!")
        super().__init__(config_file=config_file, db_details=db_details)