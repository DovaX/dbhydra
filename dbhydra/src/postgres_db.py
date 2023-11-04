
from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import BigQueryTable

from sys import platform
if platform != "linux" and platform != "linux2":
    # linux
    import psycopg2 # disable dependency for server (Temporary)


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
