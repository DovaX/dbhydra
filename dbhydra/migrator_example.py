import dbhydra_core as dh
import pandas as pd
import pymongo



db2 = dh.Mysqldb("config-mysql.ini")



'''
csv_file =  open("../examples/example.csv")
name = csv_file.name.split('/')[-1].split('.')[0]

df = pd.read_csv(csv_file)
df = df.rename(columns={'Unnamed: 0' : 'id'})
columns, types = migrator.create_migrations_from_df(name,df)
exit()
'''
db2.initialize_migrator()






#t = dh.PostgresTable(db2, "example", columns, types)
#t.create()

'''


#table1=dh.MysqlTable(db1,"test_table3",["id","test1"],["int","int"])
table2=dh.PostgresTable(db2,"test_table4",["id","test1"],["int","integer"])
table2.create()
table2.drop()
table2.create()
table2.create()

table2.add_column("new_col","int")

table2.drop_column("new_col2")
table2.drop_column("new_col")
table2.add_column("new_col2","int")

table2.modify_column("new_col2","varchar(10)")

#table2.drop()'''
#exit()

table1=dh.MysqlTable(db2,"test_table3",["id","test1"],["int","int"])
table1.create()
table1.drop()
db2.migrator.next_migration()

table1.create()
table1.create()
table1.create()




#table1.add_column("new_col","int")

table1.drop_column("new_col2")
table1.drop_column("new_col")
table1.drop_column("new_col2")

table1.add_column("new_col2","int")

table1.modify_column("new_col2","nvarchar(10)")
table1.insert([[1,"aa"]])
exit()
migration_dict={"create":{"table_name":"new_table","columns":["id","test1"],"types":["int","int"]}}
migration_dict2={"add_column":{"table_name":"new_table","column_name":"test2","column_type":"int"}}
migration_dict3={"modify_column":{"table_name":"new_table","column_name":"test2","column_type":"nvarchar(10)"}}
migration_dict4={"drop_column":{"table_name":"new_table","column_name":"test2"}}
migration_dict5={"drop":{"table_name":"new_table"}}

migration_list1=[migration_dict,migration_dict2,migration_dict3,migration_dict4,migration_dict]

db2.migrator.next_migration()

migration_list=db2.migrator.migrate_from_json("migrations//migration-1.json")
db2.migrator.migrate_from_json("migrations//migration-2.json")
print(type(migration_list))
print(migration_list)

db2.close_connection()






