from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import XlsxTable

import threading
import pathlib
import os


class XlsxDb(AbstractDb):
    """Folder-structure with .xlsx files representing database tables
    It does not need any server and runs locally with same syntax as other DB dialects
    """
    
    matching_table_class = XlsxTable
    
    def __init__(self, config_file="config.ini", db_details=None):
        self.locally=True
        if db_details is None:
            self.name="new_db"
            self.db_directory_path = None
        else:
            self.name = db_details.get("DB_DATABASE")
            self.db_directory_path = db_details.get("DB_DIRECTORY")

        self.lock = threading.Lock()
                
        if self.db_directory_path is None:
            self.db_directory_path = pathlib.Path(self.name) 
            
            
        self.python_database_type_mapping = {
        'int': "int",
        'float': "double",
        'str': "str",
        'tuple': "str",
        'list': "str",
        'dict': "str",
        'bool': "bool",
        'datetime': "datetime",
        'Jsonable': "str"
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




class XlsxDB(XlsxDb):
    """Deprecated - do not remove until dbhydra 3.x"""
    def __init__(self, config_file="config.ini", db_details=None):
        print("Deprecation warning!, XlsxDB was renamed to XlsxDb and the old name will deprecated in future!")
        super().__init__(config_file=config_file, db_details=db_details)