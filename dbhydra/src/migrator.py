import os
import math
import json
import pandas as pd

from typing import Optional
from deepdiff import DeepDiff
from dataclasses import dataclass, asdict

PENDING_MIGRATION_DEFAULT_PATH = "./db/migrations/pending_migration.json"
MIGRATION_HISTORY_DEFAULT_PATH = "./db/migrations/migration_history.json"

# @dataclass
# class Migration:
#     forward: list[dict]
#     backward: list[dict]

class Migrator:
    """
    A class for managing database migrations.

    This class provides functionality to create, manage, and execute database migrations
    using a migration system compatible with MySQL and Postgres dialects. It allows for
    creating forward and backward migrations, reading and writing migrations to JSON files,
    and executing migrations based on changes detected in database structures.

    Note: This class is compatible with MySQL and Postgres dialects and has been somewhat tested
    with those databases. It may require adjustments for other database systems.

    Attributes:
        db: The database connection object used for executing migrations.
    """
    
    def __init__(self, db):
        self.db = db
        
        # Used in older implementations, TODO: decide whether to keep both approaches, unify them or pick one
        self._migration_number = 1
        self._migration_list = []
        
        # Used in newer approach
        self._pending_forward_migration_list = []#Migration(forward=[], backward=[])
        self._pending_forward_migration_list = []#Migration(forward=[], backward=[])

    def process_migration_dict(self, migration_dict):
        matching_table_class = self.db.matching_table_class #E.g. MysqlTable
        
        assert len(migration_dict.keys()) == 1
        operation = list(migration_dict.keys())[0]
        options = migration_dict[operation]
        if operation == "create":
            table = matching_table_class(self.db, options["table_name"], options["columns"], options["types"])
            table.convert_types_from_mysql()
            table.create()
        elif operation == "drop":
            table = matching_table_class(self.db, options["table_name"])
            table.drop()
        elif operation == "add_column":
            table = matching_table_class(self.db, options["table_name"])
            table.initialize_columns()
            table.initialize_types()
            table.convert_types_from_mysql()
            table.add_column(options["column_name"], options["column_type"])
        elif operation == "modify_column":
            table = matching_table_class(self.db, options["table_name"])
            table.initialize_columns()
            table.initialize_types()
            table.convert_types_from_mysql()
            table.modify_column(options["column_name"], options["column_type"])
        elif operation == "drop_column":
            table = matching_table_class(self.db, options["table_name"])
            table.initialize_columns()
            table.initialize_types()
            table.drop_column(options["column_name"])

    # Old approach methods START
    def next_migration(self):
        self._migration_number += 1
        self._migration_list = []

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
        result = json.dumps(self._migration_list)

        if filename is None or filename == "" or filename.isspace():
            with open("migrations/migration-" + str(self._migration_number) + ".json", "w+") as f:
                f.write(result)
        else:
            with open(f"migrations/{filename}.json", "w+") as f:
                f.write(result)









    ##### Auxilliary? #####
    def create_migrations_from_df(self, name, dataframe):

        columns, return_types = self.extract_columns_and_types_from_df(dataframe)

        migration_dict = {"create": {"table_name": name, "columns": columns, "types": return_types}}
        self._migration_list.append(migration_dict)
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
    # Old approach methods END
  
    
  
    
  
    
  
    
  
    
  
    
    # def set_pending_migration(self, migration_dict: dict[str, list]):
    #     self._pending_migration = Migration(**migration_dict)
    
    def migrate_forward(self):
        """
        Applies forward migrations from the pending migration object.

        Iterates through each migration dictionary in the pending migration's forward list,
        processes the migration, saves it to migration history, and clears the pending migration.

        Returns:
            None
        """
            
        for migration_dict in self._pending_forward_migration_list:
            self.process_migration_dict(migration_dict)
            
        #self._save_migration_to_history(migration=self._pending_migration)
        self._clear_pending_migration()
            
    def migrate_backward(self):
        """
        Applies backward migrations from the pending migration object.

        Iterates through each migration dictionary in the pending migration's backward list,
        processes the migration, saves it to migration history, and clears the pending migration.

        Returns:
            None
        """
        
        for migration_dict in self._pending_backward_migration_list:
            self.process_migration_dict(migration_dict)
            
        #history_migration = Migration(forward=self._pending_migration.backward, backward=self._pending_migration.forward)
        #self._save_migration_to_history(migration=history_migration)
        self._clear_pending_migration()
        
    # def migrate_n_steps_back_in_history(self, n: int, migration_history_json: str = MIGRATION_HISTORY_DEFAULT_PATH):        
    #     migration_history = self._read_migration_history_json(migration_history_json)
        
    #     if len(migration_history) < n:
    #         raise ValueError(f"Provided n (= {n}) is larger than migration history length (= {len(migration_history)}).")

    #     total_backward_migration = Migration(forward=[], backward=[])
    #     migrations = migration_history[-n:] # Take last n elements of migration history for execution
        
    #     # Loop in reversed order as we execute backward migrations in reversed order compared to forward ones
    #     for migration_dict in reversed(migrations):
    #         total_backward_migration.forward.append(migration_dict["forward"])
    #         total_backward_migration.backward.append(migration_dict["backward"])
            
    #     self.set_pending_migration(asdict(total_backward_migration))
    #     self.migrate_backward()
    
    # def load_migration_from_json(self, json_file_path: str = PENDING_MIGRATION_DEFAULT_PATH):
    #     with open(json_file_path, "r") as file:
    #         migration_dict = json.load(file)
            
    #     self.set_pending_migration(migration_dict)
    
    # def save_pending_migration_to_json(self, file_path: str = PENDING_MIGRATION_DEFAULT_PATH):
    #     if not file_path.endswith(".json"):
    #         raise ValueError("pending migration file must be of '.json' type.")
        
    #     self._build_folder_structure_for_file_path(file_path)
        
    #     with open(file_path, "w+") as file:
    #         json.dump(asdict(self._pending_migration), file, indent=2)
    
    def create_table_migration(self, table_name: str, old_column_type_dict: Optional[dict], new_column_type_dict: Optional[dict]):
        """
        Creates a migration for a database table based on its old and new column_type_dicts.

        Args:
            table_name (str): The name of the database table.
            old_column_type_dict (Optional[dict]): The old column_type_dict of the table.
            new_column_type_dict (Optional[dict]): The new column_type_dict of the table.

            If old_column_type_dict is None and new_column_type_dict is not None: CREATE table
            If old_column_type_dict is not None and new_column_type_dict is None: DROP table

        Returns:
            Migration: The generated migration object.

        Raises:
            ValueError: If the table_name argument is empty.
        """
        
        def _extract_column_name_from_deepdiff_key(deepdiff_key: str) -> str:
            """
            Extracts the column name from a key generated by deepdiff.

            Args:
                deepdiff_key (str): The key generated by deepdiff.

            Returns:
                str: The extracted column name.

            Example:
                >>> migrator = Migrator()
                >>> column_name = migrator._extract_column_name_from_deepdiff_key("root['table']['column']")
                >>> print(column_name)
                'column'
            """
            
            # Split the item_key by '[' and ']' to isolate the column name
            # The column name is expected to be the last element after splitting
            column_name = deepdiff_key.split('[')[-1].strip("']")
            return column_name
        
        def _convert_deepdiff_dict_into_migration_lists(table_name: str, deepdiff_dict: dict):
            """
            Converts deepdiff dictionary from the new and old table column_type_dicts comparison into a Migration object.

            Args:
                table_name (str): A name of the examined DB table.
                deepdiff_dict (dict): A dictionary from DeepDiff comparison of the old and new table column_type_dict.

            Returns:
                Migration: A Migration object containing forward and backward migrations for the given table.
                
            Example:
                >>> table_name = 'results'
                >>> deepdiff_dict = {'dictionary_item_removed': {"root['hehexd']": 'double'}}
                >>> migrator = Migrator()
                >>> asdict(migrator._convert_deepdiff_dict_into_migration)
                >>> {
                    'forward': [
                        {'drop_column': {'table_name': 'results', 'column_name': 'hehexd'}}
                        ],
                    'backward': [
                        {'add_column': {'table_name': 'results', 'column_name': 'hehexd', 'column_type': 'double'}}
                        ]
                    }
            """
            forward_migration_list, backward_migration_list = [], []

            forward_conversions = {
                "dictionary_item_added": "add_column",
                "dictionary_item_removed": "drop_column",
                "values_changed": "modify_column"
                }
            backward_conversions = {
                "dictionary_item_added": "drop_column",
                "dictionary_item_removed": "add_column",
                "values_changed": "modify_column"
                }

            for action_name, deepdiff_action in deepdiff_dict.items():
                for deepdiff_key in deepdiff_action.keys():
                    column_name = _extract_column_name_from_deepdiff_key(deepdiff_key)
                    forward_action, backward_action = forward_conversions[action_name], backward_conversions[action_name]
                    
                    if action_name=="dictionary_item_added":
                        column_type = deepdiff_action[deepdiff_key]
                        forward_migration_list.append({forward_action: {"table_name": table_name, "column_name": column_name, "column_type": column_type}})
                        backward_migration_list.append({backward_action: {"table_name": table_name, "column_name": column_name}})
                    elif action_name=="dictionary_item_removed":
                        column_type = deepdiff_action[deepdiff_key]
                        forward_migration_list.append({forward_action: {"table_name": table_name, "column_name": column_name}})
                        backward_migration_list.append({backward_action: {"table_name": table_name, "column_name": column_name, "column_type": column_type}})
                    elif action_name=="values_changed":
                        column_type = deepdiff_action[deepdiff_key]["old_value"]
                        column_new_type = deepdiff_action[deepdiff_key]["new_value"]
                        
                        # HACK: Do not create migrations for cases such as varchar(2047) --> nvarchar(2047)
                        is_varchar_in_types = "varchar" in column_type and "varchar" in column_new_type
                        is_max_length_equal = (
                            column_type[column_type.index("("): column_type.index(")")] 
                            and column_new_type[column_new_type.index("("): column_new_type.index(")")]
                            ) if is_varchar_in_types else False
                        is_varchar_nvarchar_conversion = is_varchar_in_types and is_max_length_equal
                        
                        if not is_varchar_nvarchar_conversion:
                            forward_migration_list.append({forward_action: {"table_name": table_name, "column_name": column_name,
                                                                "column_type": column_new_type}})
                            backward_migration_list.append({backward_action: {"table_name": table_name, "column_name": column_name,
                                                                "column_type": column_type}})
                
            return forward_migration_list, backward_migration_list
        
        
        
        
        if not table_name:
            raise ValueError("The 'table_name' argument must be a non-empty string.")

        if not old_column_type_dict and new_column_type_dict:
            # non-empty initial column_type_dict --> empty new column_type_dict
            columns, types = list(new_column_type_dict.keys()), list(new_column_type_dict.values())
            forward_migration_list = [{"create": {"table_name": table_name, "columns": columns, "types": types}}]
            backward_migration_list = [{"drop": {"table_name": table_name}}]
            
        elif not new_column_type_dict:
            # new column_type_dict is empty ==> drop the table
            forward_migration_list = [{"drop": {"table_name": table_name}}]
            backward_migration_list = [{"create": {"table_name": table_name, "columns": columns, "types": types}}]
            
            
        else:
            diff = DeepDiff(old_column_type_dict, new_column_type_dict, verbose_level=2)
            forward_migration_list, backward_migration_list = _convert_deepdiff_dict_into_migration_lists(table_name, diff)
            
        #migration = Migration(forward=forward_migration_list, backward=backward_migration_list)
            
        self._append_migration_to_pending_migration(forward_migration_list, backward_migration_list)

        return forward_migration_list, backward_migration_list

    
    
 
    
    def _append_migration_to_pending_migration(self, forward_migration_list, backward_migration_list):
        self._pending_forward_migration_list += forward_migration_list
        self._pending_backward_migration_list += backward_migration_list
        
        
    def _clear_pending_migration(self):
        self._pending_forward_migration_list = []
        self._pending_backward_migration_list = []
        
    # def _read_migration_history_json(self, file_path: str = MIGRATION_HISTORY_DEFAULT_PATH):
    #     if not file_path.endswith(".json"):
    #         raise ValueError("Migration history file must be of '.json' type.")
        
    #     if not os.path.exists(file_path):
    #         raise FileNotFoundError(f"Migration history file '{file_path}' does not exist.")
        
    #     try:
    #         with open(file_path, "r") as file:
    #             migration_history = json.load(file)
    #     except json.JSONDecodeError:
    #         migration_history = []
            
    #     return migration_history
        
    # def _save_migration_to_history(self, migration: Migration, file_path: str = MIGRATION_HISTORY_DEFAULT_PATH):            
    #     try:
    #         migration_history = self._read_migration_history_json(file_path)
    #     except FileNotFoundError:
    #         self._build_folder_structure_for_file_path(file_path)
    #         migration_history = []
            
    #     migration_history.append(asdict(migration))
        
    #     with open(file_path, "w") as file:
    #         json.dump(migration_history, file, indent=2)
            
    # def _build_folder_structure_for_file_path(self, file_path: str):
    #     folder_path = os.path.dirname(file_path)
    #     if not os.path.exists(folder_path):
    #         print(f"Folder path to the file '{file_path}' does not exist. Creating the file and the folder structure.")
    #         os.makedirs(folder_path)
        


