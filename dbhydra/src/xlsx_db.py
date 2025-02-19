import contextlib
import os
import pathlib
import threading
from typing import Optional

from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import XlsxTable


class XlsxDb(AbstractDb):
    """Folder-structure with .xlsx files representing database tables
    It does not need any server and runs locally with same syntax as other DB dialects
    """
    
    matching_table_class = XlsxTable
    
    def __init__(self, config_file="config.ini", db_details=None, is_csv=False):
        self.locally=True
        self.is_csv = is_csv
        self.debug_mode = False #Defined because of AbstractDb method calls
        if db_details is None:
            self.name="new_db"
            self.db_directory_path = None
        else:
            self.name = db_details.get("DB_DATABASE")
            self.db_directory_path = db_details.get("DB_DIRECTORY")

        self.lock = threading.Lock()
                
        if self.db_directory_path is None:
            self.db_directory_path = pathlib.Path(self.name) 
            
        self.last_table_inserted_into: Optional[str] = None

        self.python_database_type_mapping = {
        'int': "int",
        'float': "double",
        'str': "str",
        'tuple': "str",
        'list': "str",
        'dict': "str",
        'bool': "bool",
        'datetime': "datetime",
        'Jsonable': "str",
        'Blob': "Blob"
        }

        
        class DummyXlsxConnection: #compatibility with MySQL connection
            def begin(*args):
                pass
            def commit(*args):
                pass
            def rollback(*args):
                pass
            
        class DummyXlsxCursor: #compatibility with MySQL connection
            def execute(*args):    
                pass
            
            def fetchall(*args):
                pass
            
        self.cursor=DummyXlsxCursor()
        self.connection=DummyXlsxConnection()
        self.create_database()
        
    def connect_locally(self):
        pass #no real connection
        
    def connect_remotely(self):
        pass #no real connection

    def execute(self, query):
        pass
        # self.cursor.execute(query)
        # self.cursor.commit()

    def close_connection(self):
        pass
        # self.connection.close()
        # print("DB connection closed")

    def create_database(self):
        try:
            os.mkdir(self.db_directory_path)
            print("Database directory created")
        except FileExistsError:
            print("Database directory already exists")

    @contextlib.contextmanager
    def transaction(self):
        yield None

    def get_all_tables(self):
        root,dirs,files=next(os.walk(self.db_directory_path))
        suffix=".csv" if self.is_csv else ".xlsx"
        tables = [x.lower().replace(suffix,"") for x in files]
        return (tables)

    def generate_table_dict(self, id_column_name="id"):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = XlsxTable.init_all_columns(self, table, id_column_name)
        return (table_dict)


class XlsxDB(XlsxDb):
    """Deprecated - do not remove until dbhydra 3.x"""
    def __init__(self, config_file="config.ini", db_details=None):
        print("Deprecation warning!, XlsxDB was renamed to XlsxDb and the old name will deprecated in future!")
        super().__init__(config_file=config_file, db_details=db_details)