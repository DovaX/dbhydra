import pymongo

from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import MongoTable


class MongoDb(AbstractDb):
    matching_table_class = MongoTable

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
