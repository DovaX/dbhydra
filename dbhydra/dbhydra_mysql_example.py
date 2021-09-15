import dbhydra as dh
import pandas as pd

import dbhydra_core
#db1=dh.Mysqldb() #MongoDB connect to db
db2 = dbhydra_core.MyNonSqlDb()
collections = db2.get_all_collections() #list of collections
print("Collections:" + ''.join(collections))

collection1=dbhydra_core.Collection(db2,"reviews") # db, reviews, created collection

collection1.insert(document={
        'name' : 'a',
        'rating' : 'a',
        'cuisine' : 'a'
    })
docs = collection1.find({'name': 'a'}, {"name" : 1})

for doc in docs:
    print(doc)
docs = collection1.find({'name': 'a'})
for doc in docs:
    print(doc)
print("Skuska sort")
docsSort = collection1.findSort({'name': 'a'}, "name",1, {"name" : 1})
print(type(docsSort))
for docS in docsSort:
    print(docS)

updated = collection1.update({'name': 'a'},{ "$set": { "name": "Minnie" } })
docs = collection1.find({})

for doc in docs:
    print(doc)
deleted = collection1.delete({'_id': 'ObjectId(\'6141b4398d27ba169021595d\')'})
print(deleted.deleted_count)
deleted = collection1.delete()
print(deleted.deleted_count)



print("Drop collection")
print(collection1.drop()) #return None

collections = db2.get_all_collections() #list of collections
print("Collections:" + ''.join(collections))

#table1.create()

#rows=[[2,2]]

#df=pd.DataFrame(rows,columns=["test2","test3"])

#table1.insert_from_df(df)

#rows=table1.select_all()


#table1.drop() ?

#db1.close_connection()