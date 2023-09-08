"""Sample model"""
from dbhydra import *
db1=db()

table1 = Table(db1,"test",["test1","test2","test3","test4"],["int","int","int","int"])
#table1.drop()
#table1.create()
#rows=[[1,2,3,4],[5,4,7,9]]
#table1.insert(rows)

list1=table1.select("SELECT * FROM test")
print(list1)

#list2=table1.select_all()
#print(list2)

#table1.drop()


table1.export_to_xlsx()




tables=db1.get_all_tables()
table_dict=db1.generate_table_dict()
print(tables)


columns=table_dict['test'].get_all_columns()
types=table_dict['test'].get_all_types()
print(columns,types)

table_test=Table.init_all_columns(db1,"test")

print(table_test.columns)


table2 = Table(db1,"test_new",["id","test2"],["int","nvarchar(20)"])
#table2.create()


