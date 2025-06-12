


import dbhydra.dbhydra_core as dh


db1 = dh.MysqlDb("config-mysql.ini")


import pandas as pd


real_estate_df = pd.read_excel("cz_real_estate_data_sample.xlsx", index_col = 0)
real_estate_columns = ["id"]+list(real_estate_df.columns)

types = ["int", "nvarchar(100)","nvarchar(100)","nvarchar(20)","int","nvarchar(1000)","int"]

with db1.connect_to_db():
    
    
    real_estate_table = dh.MysqlTable(db1, "real_estate", columns = real_estate_columns, types = types)
    
    #real_estate_table.create()
    
    new_df=real_estate_table.select_to_df() #selecting
    #real_estate_table.insert_from_df() #inserting
    
    
    
    #table_dict = db1.generate_table_dict("id") #getting list of all tables in db
    #print(table_dict)
    