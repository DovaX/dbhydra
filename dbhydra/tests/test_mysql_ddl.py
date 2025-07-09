##### DDL (data definition language) tests for MySQL #####

import os
import pytest
import random
import string
import dbhydra.dbhydra_core as dh

def random_table_name(prefix="test_table_"):
    return prefix + ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

# Rename mysqldb fixture and all references to db1
@pytest.fixture(scope="module")
def db1():
    # Get the directory of this test file, then go up one level to the project root
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    config_path = os.path.join(root_dir, "config-mysql.ini")
    return dh.MysqlDb(config_file=config_path)

@pytest.fixture(scope="function")
def temp_mysql_table(db1):
    table_name = random_table_name()
    columns = ["id", "name"]
    types = ["int", "varchar(255)"]
    table = dh.MysqlTable(db1, table_name, columns, types)
    with db1.connect_to_db():
        table.create()
        table_dict = db1.generate_table_dict()
        assert table_name in table_dict, f"Temp table {table_name} was not created!"
    yield table
    # Cleanup
    try:
        with db1.connect_to_db():
            table.drop()
            table_dict = db1.generate_table_dict()
            assert table_name not in table_dict, f"Temp table {table_name} was not dropped!"
    except Exception as e:
        print(f"[CLEANUP] Could not drop table {table_name}: {e}")
        raise

def test_mysql_create_and_drop_table(db1):
    table_name = random_table_name()
    columns = ["id", "name"]
    types = ["int", "varchar(255)"]
    table = dh.MysqlTable(db1, table_name, columns, types)
    with db1.connect_to_db():
        table.create()
        table_dict = db1.generate_table_dict()
        assert table_name in table_dict, f"Table {table_name} was not created!"
        table.drop()
        table_dict = db1.generate_table_dict()
        assert table_name not in table_dict, f"Table {table_name} was not dropped!"

def test_mysql_add_column(temp_mysql_table, db1):
    table = temp_mysql_table
    column = "age"
    type = "int"
    with db1.connect_to_db():
        table.add_column(column, type)
        df = table.select_to_df()
        columns = df.columns.tolist()
        assert column in columns, f"Column '{column}' was not added! Columns: {columns}"

def test_mysql_drop_column(temp_mysql_table, db1):
    table = temp_mysql_table
    column = "age"
    type = "int"
    with db1.connect_to_db():
        table.add_column(column, type)
        df = table.select_to_df()
        columns = df.columns.tolist()
        assert column in columns, f"Column '{column}' was not added! Columns: {columns}"
        table.drop_column(column)
        df = table.select_to_df()
        columns = df.columns.tolist()
        assert column not in columns, f"Column '{column}' was not dropped! Columns: {columns}"

def test_mysql_modify_column(temp_mysql_table, db1):
    table = temp_mysql_table
    column = "age"
    type = "int"
    with db1.connect_to_db():
        table.add_column(column, type)
        df = table.select_to_df()
        columns = df.columns.tolist()
        assert column in columns, f"Column '{column}' was not added! Columns: {columns}"
        # Modify column type
        type2 = "varchar(100)"
        table.modify_column(column, type2)
        types = table.get_all_types()
        assert any("varchar" in t for t in types), f"Column '{column}' was not modified to varchar! Types: {types}" 