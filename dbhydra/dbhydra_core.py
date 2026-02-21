"""DB Hydra ORM"""
import abc
from typing import Any

import pandas as pd
from pydantic import BaseModel

##### Do not remove imports - they are expored in the package
from dbhydra.src.sqlserver_db import SqlServerDb, db
from dbhydra.src.mysql_db import MysqlDb, Mysqldb
from dbhydra.src.bigquery_db import BigQueryDb
from dbhydra.src.mongo_db import MongoDb
from dbhydra.src.postgres_db import PostgresDb
from dbhydra.src.xlsx_db import XlsxDb, XlsxDB
from dbhydra.src.abstract_db import AbstractDb
from dbhydra.src.tables import (SqlServerTable, PostgresTable, MysqlTable, XlsxTable, AbstractTable, MongoTable,
                                BigQueryTable, Table, AbstractSelectable, AbstractJoinable, PYTHON_TO_MYSQL_DATA_MAPPING)
##### Do not remove imports - they are expored in the package


class Jsonable(str):
    """Is used as type in python_database_type_mapping"""    
    pass

class Blob(str):
    """Store BLOBs up to 16MB."""    
    pass

class LongText(str):
    """Store long text strings (maps to MySQL LONGTEXT)."""    
    pass


class LongText(str):
    """Marker type for columns that should be mapped to LONGTEXT in SQL backends."""
    pass


# dataframe - dictionary auxiliary functions
def df_to_dict(df, column1, column2):
    dictionary = df.set_index(column1).to_dict()[column2]
    return (dictionary)


def dict_to_df(dictionary, column1, column2):
    df = pd.DataFrame(list(dictionary.items()), columns=[column1, column2])
    return (df)


print("test")
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
