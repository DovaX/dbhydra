import abc
import threading
from contextlib import contextmanager
from typing import Optional

from dbhydra.src.migrator import Migrator
from dbhydra.src.tables import AbstractTable


def read_connection_details(config_file):
    def read_file(file):
        """Reads txt file -> list"""
        with open(file, "r") as f:
            rows = f.readlines()
            for i, row in enumerate(rows):
                rows[i] = row.replace("\n", "")
        return (rows)
    
    connection_details = read_file(config_file)
    db_details = {}
    for detail in connection_details:
        key = detail.split("=")[0]
        value = detail.split("=")[1]
        db_details[key] = value

        # Skip empty lines to avoid error when reading config file
        if not detail:
            continue

    print(", ".join([db_details['DB_SERVER'], db_details['DB_DATABASE'], db_details['DB_USERNAME']]))
    return (db_details)





class Transaction:
    def __init__(self, connection):
        self.connection = connection

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()




class AbstractDb(abc.ABC):
    matching_table_class = AbstractTable
    
    typing_to_python_mapping = {
        'List': 'list',
        'Dict': 'dict',
        'Tuple': 'tuple',
        'Set': 'set',
        'Union': 'object',
        'Optional': 'object',
        'Jsonable': 'Jsonable',
        'Blob': 'Blob',
        # 'FrozenSet': frozenset,
        # 'Deque': list,
        # 'Any': object,
        #
        # 'Callable': object,
        # 'Type': type,
        # 'TypeVar': object,
        # 'Generic': object,
        # 'Sequence': list,
        # 'Iterable': list,
        # 'Mapping': dict,
        # 'AbstractSet': set,
    }
    python_database_type_mapping = {}
    def __init__(self, config_file="config.ini", db_details=None):
        if db_details is None:
            db_details = read_connection_details(config_file)

        self.locally = True
        if db_details["LOCALLY"] == "False":
            self.locally = False

        self.DB_SERVER = db_details["DB_SERVER"]
        self.DB_DATABASE = db_details["DB_DATABASE"]
        self.DB_USERNAME = db_details["DB_USERNAME"]
        self.DB_PASSWORD = db_details["DB_PASSWORD"]
        
        self.is_autocommiting=True

        if "DB_PORT" in db_details.keys():
            self.DB_PORT = int(db_details["DB_PORT"])
        else:
            self.DB_PORT = None

        if "DB_DRIVER" in db_details.keys():
            self.DB_DRIVER = db_details["DB_DRIVER"]
        else:
            self.DB_DRIVER = "ODBC Driver 13 for SQL Server"

        self.lock = threading.Lock()

        # This call to `self.connect_to_db` is not doing anything as it returns a
        # context manager object
        # self.connect_to_db()
        
        self.active_transactions=[]
        self.last_table_inserted_into: Optional[str] = None
        self.identifier_quote = ''

    @abc.abstractmethod
    def connect_locally(self):
        pass

    @abc.abstractmethod
    def connect_remotely(self):
        pass

    def _connect(self):
        if self.locally:
            self.connect_locally()
        else:
            self.connect_remotely()

    def connect(self):
        print("DEPRECATION WARNING: use `connect_to_db` context manager instead of `connect` method")
        if self.locally:
            self.connect_locally()
        else:
            self.connect_remotely()

    #@abc.abstractmethod
    def get_all_tables(self):
        pass

    #@abc.abstractmethod
    def generate_table_dict(self):
        pass

    def get_table(self, table_name: str):
        """
        Retrieves Table from DB using its name.
        """

        try:
            table = self.generate_table_dict()[table_name]
        except KeyError:
            print(f'Table "{table_name}" was not found in the DB.')
            raise KeyError

        return table

    @contextmanager
    def connect_to_db(self):
        try:
            self._connect()
            yield None
        finally:
            self.close_connection()

    @contextmanager
    def connect_to_table(self, table_name):
        with self.connect_to_db():
            table = self.generate_table_dict()[table_name]

            yield table


    @contextmanager
    def transaction(self):
        self.is_autocommiting=False
        transaction = Transaction(self.connection)
        self.active_transactions.append(transaction)
        try:
            yield transaction
        except Exception as e:
            transaction.rollback()
            raise e
        else:
            transaction.commit()
        finally:
            self.active_transactions.remove(transaction)
            if len(self.active_transactions)==0:
                self.is_autocommiting=True


    def execute(self, query):
        result=self.cursor.execute(query)
        if self.is_autocommitting:
            self.cursor.commit()
        return(result)

    def close_connection(self):
        self.connection.close()
        print("DB connection closed")

    def initialize_migrator(self):
        self.migrator = Migrator(self)

    def _convert_column_type_dict_from_python(self, column_type_dict):
        """
        First apply mapping from python typing module to standard python.
        Then apply mapping from python to database types
        """
        typing_python_mapping = {column_name: self.typing_to_python_mapping.get(column_type,column_type) for column_name, column_type in column_type_dict.items()}
        return {column_name: self.python_database_type_mapping[column_type] for column_name, column_type in typing_python_mapping.items()}

