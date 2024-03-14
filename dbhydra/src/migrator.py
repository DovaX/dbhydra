import pandas as pd
import math
import json

@dataclass
class Migration:
    forward: list[dict]
    backward: list[dict]

class Migrator:
    """It was somewhat tested only for MySQL and Postgres dialect"""
    
    def __init__(self, db):
        self.db = db
        self.migration_number = 1
        self.migration_list = []
        self.current_migration = Migration(forward=[], backward=[])

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
    
    def set_current_migration(self, migration_dict: dict[str, list]):
        self.current_migration = Migration(**migration_dict)
    def create_table_migration(self, table_name: str, old_structure: Optional[dict], new_structure: Optional[dict]):
        """
        Creates a migration for a database table based on its old and new structures.

        Args:
            table_name (str): The name of the database table.
            old_structure (Optional[dict]): The old structure of the table.
            new_structure (Optional[dict]): The new structure of the table.

            If old_structure is None and new_structure is not None: CREATE table
            If old_structure is not None and new_structure is None: DROP table

        Returns:
            Migration: The generated migration object.

        Raises:
            ValueError: If the table_name argument is empty.
        """
    
        if not table_name:
            raise ValueError("The 'table_name' argument must be a non-empty string.")

        if not old_structure and new_structure:
            # non-empty initial structure --> empty new structure
            columns, types = list(new_structure.keys()), list(new_structure.values())
            forward_migration = [{"create": {"table_name": table_name, "columns": columns, "types": types}}]
            backward_migration = [{"drop": {"table_name": table_name}}]
            
            migration = Migration(forward=forward_migration, backward=backward_migration)
        elif not new_structure:
            # new structure is empty ==> drop the table
            forward_migration = [{"drop": {"table_name": table_name}}]
            backward_migration = [{"create": {"table_name": table_name, "columns": columns, "types": types}}]
            
            migration = Migration(forward=forward_migration, backward=backward_migration)
        else:
            diff = DeepDiff(old_structure, new_structure, verbose_level=2)
            migration = self._convert_deepdiff_dict_into_migration(table_name, diff)
            
        self._merge_migration_to_current_migration(migration=migration)

        return migration

    def _convert_deepdiff_dict_into_migration(self, table_name: str, deepdiff_dict: dict) -> Migration:
        """
        Converts deepdiff dictionary from the new and old table structures comparison into a Migration object.

        Args:
            table_name (str): A name of the examined DB table.
            deepdiff_dict (dict): A dictionary from DeepDiff comparison of the old and new table structure.

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
        forward_migration, backward_migration = [], []

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
                column_name = self._extract_column_name_from_deepdiff_key(deepdiff_key)
                forward_action, backward_action = forward_conversions[action_name], backward_conversions[action_name]
                
                if action_name=="dictionary_item_added":
                    column_type = deepdiff_action[deepdiff_key]
                    forward_migration.append({forward_action: {"table_name": table_name, "column_name": column_name, "column_type": column_type}})
                    backward_migration.append({backward_action: {"table_name": table_name, "column_name": column_name}})
                elif action_name=="dictionary_item_removed":
                    column_type = deepdiff_action[deepdiff_key]
                    forward_migration.append({forward_action: {"table_name": table_name, "column_name": column_name}})
                    backward_migration.append({backward_action: {"table_name": table_name, "column_name": column_name, "column_type": column_type}})
                elif action_name=="values_changed":
                    column_type = deepdiff_action[deepdiff_key]["old_value"]
                    column_new_type = deepdiff_action[deepdiff_key]["new_value"]
                    forward_migration.append({forward_action: {"table_name": table_name, "column_name": column_name,
                                                        "column_type": column_new_type}})
                    backward_migration.append({backward_action: {"table_name": table_name, "column_name": column_name,
                                                        "column_type": column_type}})
            
        return Migration(forward=forward_migration, backward=backward_migration)
    
    def _extract_column_name_from_deepdiff_key(self, deepdiff_key: str) -> str:
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
    
    def _merge_migration_to_current_migration(self, migration: Migration):
        new_forward_part = self.current_migration.forward + migration.forward
        new_backward_part = self.current_migration.backward + migration.backward
        self.current_migration = Migration(forward=new_forward_part, backward=new_backward_part)
        
    def _clear_current_migration(self):
        self.current_migration = Migration(forward=[], backward=[])
        
    def _read_migration_history_json(self, file_path: str = MIGRATION_HISTORY_DEFAULT_PATH):
        if not file_path.endswith(".json"):
            raise ValueError("Migration history file must be of '.json' type.")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Migration history file '{file_path}' does not exist.")
        
        try:
            with open(file_path, "r") as file:
                migration_history = json.load(file)
        except json.JSONDecodeError:
            migration_history = []
            
        return migration_history
        
    def _save_migration_to_history(self, migration: Migration, file_path: str = MIGRATION_HISTORY_DEFAULT_PATH):            
        try:
            migration_history = self._read_migration_history_json(file_path)
        except FileNotFoundError:
            self._build_folder_structure_for_file_path(file_path)
            migration_history = []
            
        migration_history.append(asdict(migration))
        
        with open(file_path, "w") as file:
            json.dump(migration_history, file, indent=2)
            
    def _build_folder_structure_for_file_path(self, file_path: str):
        folder_path = os.path.dirname(file_path)
        if not os.path.exists(folder_path):
            print(f"Folder path to the file '{file_path}' does not exist. Creating the file and the folder structure.")
            os.makedirs(folder_path)
        


