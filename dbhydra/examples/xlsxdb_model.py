from dbhydra_core import XlsxDB,XlsxTable
import pandas as pd

df=pd.DataFrame([[1,2,3],[1,3,4],[1,5,6],[1,7,8]])


db1=XlsxDB("New DB")
db1.create_database()

accounts_table=XlsxTable(db1,"accounts",columns=["id","A","B","C"])


#df2=accounts_table.select_to_df()
#print(df2)
accounts_table.insert_from_df(df)



#ccounts_table.update("A=99","B=2")

accounts_table.delete("C=4")
blabla=accounts_table.select_to_df()
#df.to_excel(db1.name+"\\"+accounts_table.name+".xlsx")
#print(df)



print(blabla)