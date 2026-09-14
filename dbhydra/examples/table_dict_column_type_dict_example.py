import dbhydra.dbhydra_core as dh


db1 = dh.MysqlDb("config-mysql.ini")


with db1.connect_to_db():
    table_dict = db1.generate_table_dict()
    print("Tables in database:", list(table_dict.keys()))

    test1_table = table_dict["test1"]
    print("columns:", test1_table.get_all_columns())
    print("types:", test1_table.get_all_types())
    print("column_type_dict:", test1_table.column_type_dict)

    new_column_type_dict = {
        "id": "int",
        "test": "int",
        "note": "nvarchar",
    }

    test2_table = dh.MysqlTable.init_from_column_type_dict(
        db1, "test2", new_column_type_dict
    )
    print(f"\nNew table test2 column_type_dict:", test2_table.column_type_dict)

    #new_table.create()
    #new_table.drop()
