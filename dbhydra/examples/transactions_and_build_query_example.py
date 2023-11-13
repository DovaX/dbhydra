

from dbhydra.dbhydra_core import *
#.src.tables import MysqlTable


table1=MysqlTable(db1,"prices") #database needs to be specified
table1.query_building_enabled=True
df=table1.select("price, name").where("price",5).where("name","John",operator="=").to_df().execute()






rows=table1.select("*").where("uid",10).where("project_uid",20)

with db1.transaction() as t:
    df=table1.select_to_df()
    table1.delete()
    