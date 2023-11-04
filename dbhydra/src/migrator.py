import pandas as pd
import math
import json



class Migrator:
    def __init__(self, db=None):
        self.db = db
        self.migration_number = 1
        self.migration_list = []

    def process_migration_dict(self, migration_dict):
        matching_table_class = self.db.matching_table_class #E.g. MysqlTable
        
        assert len(migration_dict.keys()) == 1
        operation = list(migration_dict.keys())[0]
        options = migration_dict[operation]
        if operation == "create":
            if (isinstance(self.db, Mysqldb)):
                table = MysqlTable(self.db, options["table_name"], options["columns"], options["types"])
            elif (isinstance(self.db, PostgresDb)):
                table = PostgresTable(self.db, options["table_name"], options["columns"], options["types"])
            table.convert_types_from_mysql()
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
            table.convert_types_from_mysql()
            table.add_column(options["column_name"], options["column_type"])
        elif operation == "modify_column":
            if (isinstance(self.db, Mysqldb)):
                table = MysqlTable(self.db, options["table_name"])
            elif (isinstance(self.db, PostgresDb)):
                table = PostgresTable(self.db, options["table_name"])
            table.initialize_columns()
            table.initialize_types()
            table.convert_types_from_mysql()
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
            with open(f"migrations/{filename}.json", "w+") as f:
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

        if columns == []:
            return ["id"], ["int"]

        for column in dataframe:
            if dataframe.empty:
                return_types.append(type(None).__name__)
                continue

            t = dataframe.loc[0, column]
            try:
                if pd.isna(t):
                    return_types.append(type(None).__name__)
                else:
                    try:
                        return_types.append(type(t.item()).__name__)
                    except:
                        return_types.append(type(t).__name__)
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

