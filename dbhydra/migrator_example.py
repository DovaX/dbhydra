import dbhydra_core as dh


db1=dh.Mysqldb("config-mysql.ini")

db1.initialize_migrator()



table1=dh.MysqlTable(db1,"test_table3",["id","test1"],["int","int"])

table1.create()

db1.migrator.next_migration()

table1.create()
table1.create()
table1.create()


"""

#table1.add_column("new_col","int")

#table1.drop_column("new_col2")
#table1.drop_column("new_col2")
table1.add_column("new_col2","int")

table1.modify_column("new_col2","nvarchar(10)")

"""

migration_dict={"create":{"table_name":"new_table","columns":["id","test1"],"types":["int","int"]}}
migration_dict2={"add_column":{"table_name":"new_table","column_name":"test2","column_type":"int"}}
migration_dict3={"modify_column":{"table_name":"new_table","column_name":"test2","column_type":"nvarchar(10)"}}
migration_dict4={"drop_column":{"table_name":"new_table","column_name":"test2"}}
migration_dict5={"drop":{"table_name":"new_table"}}

migration_list=[migration_dict,migration_dict2,migration_dict3,migration_dict4,migration_dict]



migration_list=db1.migrator.migrate_from_json("migrations//migration-2.json")
print(migration_list)

db1.close_connection()






