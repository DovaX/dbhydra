
import dbhydra.dbhydra_core as dh
import pandas as pd



db1=dh.MysqlDb("config-mysql-local.ini")


with db1.connect_to_db():
    print("hello")
    
    
    test123_table = dh.MysqlTable(db1, "test123",["id","test"],["int","int"])
    #test123_table.create()
    
    df=pd.DataFrame([[2,3],[5,6]],columns =["test", "test2"])
    df=df.drop("test2", axis=1)
    test123_table.insert_from_df(df)
    
    test123_table.update("test = 456", debug_mode = True)