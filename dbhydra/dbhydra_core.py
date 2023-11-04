"""DB Hydra ORM"""
import abc
import json
import math
import sys
import threading
#from copy import copy
from sys import platform
import os
import pathlib
from typing import Any, Optional

import numpy as np
import pandas as pd
import pymongo
from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseModel

#! Disabled on macOS --> problematic import
if sys.platform != "darwin":
    import pyodbc

if platform != "linux" and platform != "linux2":
    # linux
    import psycopg2 # disable dependency for server (Temporary)



from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import Table, PostgresTable, MysqlTable, XlsxTable, AbstractTable, MongoTable, BigQueryTable





# class DataMigrator:





# AWFUL NAME, SHOULD BE RENAMED WITH MAJOR RELEASE (This connects basic SQL i suppose)


class Jsonable(str):
    pass




class PostgresDb(AbstractDb):

    def connect_locally(self):
        self.connection = psycopg2.connect(
            host=self.DB_SERVER,
            database=self.DB_DATABASE,
            user=self.DB_USERNAME,
            password=self.DB_PASSWORD)
        self.cursor = self.connection.cursor()

    def connect_remotely(self):
        self.connection = psycopg2.connect(
            host=self.DB_SERVER,
            database=self.DB_DATABASE,
            user=self.DB_USERNAME,
            password=self.DB_PASSWORD)
        self.connection.autocommit = True
        self.cursor = self.connection.cursor()
        
    def close_connection(self):
        self.cursor.close()
        print("DB connection closed")

    def execute(self, query, is_autocommitting=True):
        result=self.cursor.execute(query)
        if is_autocommitting:
            self.connection.commit()
        return result

        # return  [''.join(i) for i in self.cursor.fetchall()]

    def get_all_tables(self):
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
               WHERE table_schema = 'public'""")
        tables = [''.join(x) for x in self.cursor.fetchall()]
        return tables

    def generate_table_dict(self):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = PostgresTable.init_all_columns(self, table)

        return (table_dict)


class BigQueryDb(AbstractDb):
    def __init__(self, db_details):
        self.credentials_path = db_details["DB_SERVER"]
        self.dataset = db_details["DB_DATABASE"]


        self.locally = False

        self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path, scopes=[
            "https://www.googleapis.com/auth/cloud-platform"], )


    def connect_remotely(self):
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)

    def connect_locally(self):
        raise Exception("Cannot connect locally to Big Query")


    def close_connection(self):
        self.client.close()

    def select_to_df(self, query):
        print(query)
        df = pd.read_gbq(query=query, project_id=self.project_id, credentials=self.credentials)

        return df

    def get_all_tables(self):
        query = f"""
        SELECT table_name
        FROM {self.dataset}.INFORMATION_SCHEMA.TABLES
        """
        rows = list(self.client.query(query))
        table_names = [row[0] for row in rows]
        return table_names

    def generate_table_dict(self):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = BigQueryTable.init_all_columns(self, table)

        return (table_dict)




    def execute(self, query):
        query_job = self.client.query(query)
        results = query_job.result()  # Waits for job to complete.
        return results


class MongoDb(AbstractDb):

    def connect_locally(self):
        self.connection = pymongo.MongoClient(
            host=self.DB_SERVER,
            authSource=self.DB_DATABASE,
            username=self.DB_USERNAME,
            password=self.DB_PASSWORD,
            authMechanism='SCRAM-SHA-256'
        )

        self.database = self.connection.get_database(self.DB_DATABASE)
        print('Connected locally')

    def connect_remotely(self):
        self.connection = pymongo.MongoClient(
            "mongodb+srv://" + self.DB_USERNAME + ":" + self.DB_PASSWORD + "@" + self.DB_SERVER + "/" + self.DB_DATABASE + "?retryWrites=true&w=majority")
        print(self.connection.list_database_names())
        self.database = self.connection[self.DB_DATABASE]
        print(self.database)
        print("DB connection established")

    def execute(self, query, is_autocommitting=True):
        result=self.cursor.execute(query)
        if is_autocommitting:
            self.connection.commit()
        return(result)
    
    def get_all_tables(self):
        return self.database.list_collection_names()

    def create_table(self, name):
        table = self.database[name]
        return table

    def createTable(self, name):
        print("WARNING: `createTable` method will be deprecated in favor of `create_table`")
        return self.create_table(name)

    def close_connection(self):
        self.connection.close()
        print("DB connection closed.")

    def generate_table_dict(self):
        tables = self.get_all_tables()
        table_dict = dict()
        for i, table in enumerate(tables):
            table_dict[table] = MongoTable.init_all_columns(self, table)

        return (table_dict)


class XlsxDB(AbstractDb):
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


# dataframe - dictionary auxiliary functions
def df_to_dict(df, column1, column2):
    dictionary = df.set_index(column1).to_dict()[column2]
    return (dictionary)


def dict_to_df(dictionary, column1, column2):
    df = pd.DataFrame(list(dictionary.items()), columns=[column1, column2])
    return (df)



from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler

class AbstractModel(abc.ABC, BaseModel):
    @classmethod
    def generate_dbhydra_table(cls, table_class: AbstractTable, db1, name, id_column_name="id"):
        column_type_dict = create_table_structure_dict(cls, id_column_name=id_column_name)
        dbhydra_table = table_class.init_from_column_type_dict(db1, name, column_type_dict, id_column_name=id_column_name)
        return dbhydra_table
    
    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))


 

def create_table_structure_dict(api_class_instance, id_column_name="id"):
    """
    Accepts instance of API data class (e.g. APIDatabase) and converts it to dictionary {attribute_name: attribute_type}
    """
    table_structure_dict = {id_column_name: "int"}
    table_structure_dict = {**table_structure_dict,
                            **{attribute_name: attribute_type.__name__ for attribute_name, attribute_type in api_class_instance.__annotations__.items()}}

    return table_structure_dict
