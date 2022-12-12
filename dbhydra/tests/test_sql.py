import pytest
import dbhydra.dbhydra_core as dh
import pandas as pd
import keepvariable.keepvariable_core as kv

import dbhydra.tests.test_cases as test_cases

db_details_mysql = kv.load_variable_safe("tests/test_sql_db.kv", varname="db_details")
db_details_postgres = kv.load_variable_safe(
    "tests/test_postgres_db.kpv", varname="db_details"
)


@pytest.fixture(scope="module")
def mysqldb():
    return dh.Mysqldb(db_details=db_details_mysql)


@pytest.fixture(scope="module")
def postgresdb():
    return dh.PostgresDb(db_details=db_details_postgres)


@pytest.mark.parametrize("db", [("mysqldb"), ("postgresdb")])
def test_mysql_server_connection_and_table_presence(db, request):
    db = request.getfixturevalue(db)
    with db.connect_to_db():
        tables = db.generate_table_dict()

    assert "testing_table" in tables


@pytest.mark.parametrize(
    "db,table_name,compare_df",
    [
        (
            "mysqldb",
            "testing_table",
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "value1": [100, 100, 100],
                    "value2": ["slon", "slon", "slon"],
                }
            ).astype("object"),
        ),
        (
            "mysqldb",
            "testing_table_insert",
            pd.DataFrame([], columns=["id", "name", "age"]).astype("object"),
        ),
        (
            "postgresdb",
            "testing_table",
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "value1": [100, 100, 100],
                    "value2": ["slon", "slon", "slon"],
                }
            ).astype("object"),
        ),
        (
            "postgresdb",
            "testing_table_insert",
            pd.DataFrame([], columns=["id", "name", "age"]).astype("object"),
        ),
    ],
)
def test_mysql_select(db, table_name, compare_df, request):
    db = request.getfixturevalue(db)
    with db.connect_to_db():
        tables = db.generate_table_dict()
        testing_table = tables[table_name]
        response_df = testing_table.select_to_df().astype("object")

    assert response_df.equals(compare_df.reindex(columns=response_df.columns))



@pytest.mark.parametrize(
    "db,input_df,compare_df,seq,",
    [
        ("mysqldb", test_cases.test_case1[0], test_cases.test_case1[1], test_cases.test_case1[2]),
        ("postgresdb", test_cases.test_case1[0], test_cases.test_case1[1], test_cases.test_case1[2]),
    ],
)
def test_mysql_crud(db, input_df, compare_df, seq, request):
    db = request.getfixturevalue(db)

    with db.connect_to_db():
        tables = db.generate_table_dict()
        testing_table = tables["testing_table_insert"]
        testing_table.insert_from_df(input_df)
        seq(testing_table, "age=5", "name='Lisa'", "age=100", "age=144")
        response_df = testing_table.select_to_df()
        testing_table.delete()

    response_rows = response_df.drop(["id"], axis=1).astype("object")

    assert response_rows.equals(compare_df)