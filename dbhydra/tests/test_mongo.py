import pytest
import dbhydra.dbhydra_core as dh
import pandas as pd
import keepvariable.keepvariable_core as kv

import dbhydra.tests.test_cases as test_cases



db_details_mongo = kv.load_variable_safe("tests/test_mongo_db.kpv", varname="db_details")


@pytest.fixture(scope="module")
def mongodb():
    return dh.MongoDb(db_details=db_details_mongo)


def test_mongo_server_connection_and_table_presence(mongodb):
    with mongodb.connect_to_db():
        tables = mongodb.get_all_tables()

    assert "testing_table" in tables


@pytest.mark.parametrize(
    "db,compare_df, table_name",
    [
        ("mongodb", pd.DataFrame({"name": ["John"]}).astype("object"), "testing_table"),
        ("mongodb", pd.DataFrame([]).astype("object"), "testing_table_insert"),
    ],
)
def test_mongo_select(db, compare_df, table_name, request):
    db = request.getfixturevalue(db)
    with db.connect_to_db():
        tables = db.generate_table_dict()
        testing_table = tables[table_name]
        response_df = testing_table.select_to_df().astype("object")

    if "_id" in response_df.columns:
        response_df = response_df.drop(["_id"], axis=1)

    assert response_df.equals(compare_df)


@pytest.mark.parametrize(
    "db,input_df,compare_df,seq,",
    [
        ("mongodb", test_cases.test_case1[0], test_cases.test_case1[1], test_cases.test_case1[2]),
    ],
)
def test_mongo_crud(db, input_df, compare_df, seq, request):
    db = request.getfixturevalue(db)
    with db.connect_to_db():
        tables = db.generate_table_dict()
        testing_table = tables['testing_table_insert']
        testing_table.insert_from_df(input_df)
        seq(testing_table, {'age': 5}, {'name' : 'Lisa'}, {"$set": {"age":100}}, { "age": 144})

        resp_df = testing_table.select_to_df().astype('object')
        testing_table.delete()

    resp_df = resp_df.drop(['_id'], axis=1)

    assert resp_df.equals(compare_df)
