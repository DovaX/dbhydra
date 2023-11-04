import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import BigQueryTable


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
