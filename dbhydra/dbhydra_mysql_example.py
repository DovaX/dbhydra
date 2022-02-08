

import dbhydra_core
from random import randint
#db1=dh.Mysqldb() #MongoDB connect to db a
#db2 = dbhydra_core.MongoDb()
db3 = dbhydra_core.PostgresDb()

tables = db3.get_all_tables()
print(tables)
print(tables[6])
table = tables[6]
tableP = dbhydra_core.PostgresTable(db3, table)

print("select")
print(tableP.select_all())
print(tableP.select_to_df())
c = tableP.get_all_columns()
print(tableP.get_all_types())
print("init")
s = tableP.init_all_columns(db3, table)
print(s)
print(db3.generate_table_dict())
print(tableP.get_all_types())
print(c)
db3.close_connection()
exit()
#collections = db2.get_all_tables() #list of collections
#print("Collections:" + ''.join(collections))
a = {
    "url": "google.com",
    "statusCode": 301,
    "headers": {
        "location": "http://www.google.com/",
        "content-type": "text/html; charset=UTF-8",
        "date": "Fri, 22 Mar 2013 16:27:55 GMT",
        "expires": "Sun, 21 Apr 2013 16:27:55 GMT",
        "cache-control": "public, max-age=2592000",
        "server": "gws",
        "content-length": "219",
        "x-xss-protection": "1; mode=block",
        "x-frame-options": "SAMEORIGIN"
    }
}
b = {
    "url": 5,
    "statusCode": 301,
    "headers": {
        "location": "http://www.google.com/",
        "content-type": "text/html; charset=UTF-8",
        "date": "Fri, 22 Mar 2013 16:27:55 GMT",
        "expires": "Sun, 21 Apr 2013 16:27:55 GMT",
        "cache-control": "public, max-age=2592000",
        "server": "gws",
        "content-length": "219",
        "x-xss-protection": "1; mode=block",
        "x-frame-options": "SAMEORIGIN"
    }
}
collection1=dbhydra_core.MongoTable(db2,"reviews") # db, reviews, created collection
collection1.drop()
collection1.insert(a)
collection1.insert(b)
print(collection1.select_all({}))
print(collection1.select_to_df({}))
print(collection1.get_columns_types())

exit()
names = ['Kitchen','Animal','Statye', 'Tastey', 'Big','City','Fish', 'Pizza','Goat', 'Salty','Sandwich','Lazy', 'Fun']
company_type = ['LLC','Inc','Company','Corporation']
company_cuisine = ['Pizza', 'Bar Food', 'Fast Food', 'Italian', 'Mexican', 'American', 'Sushi Bar', 'Vegetarian']
for x in range(1, 501):

    business = {
        'name' : names[randint(0, (len(names)-1))] + ' ' + names[randint(0, (len(names)-1))]  + ' ' + company_type[randint(0, (len(company_type)-1))],
        'rating' : randint(1, 5),
        'cuisine' : company_cuisine[randint(0, (len(company_cuisine)-1))]
    }
    #Step 3: Insert business object directly into MongoDB via insert_one
    collection1.insert(business)


collection1.insert(document={
        'name' : 'a',
        'rating' : 'a',
        'cuisine' : 'a'
    })
docs = collection1.select({'name': 'a'}, {"name" : 1})
print(type(docs))
for doc in docs:
    print(doc)
docs = collection1.select({'name': 'a'})
for doc in docs:
    print(doc)
print("Skuska sort")
docsSort = collection1.selectSort({'name': 'a'}, "name",1, {"name" : 1})
print(type(docsSort))
for docS in docsSort:
    print(docS)

updated = collection1.update({'name': 'a'},{ "$set": { "name": "Minnie" } })
docs = collection1.select({})

for doc in docs:
    print(doc)
deleted = collection1.delete({'_id': 'ObjectId(\'6141b4398d27ba169021595d\')'})
print(deleted.deleted_count)
#deleted = collection1.delete()
print(deleted.deleted_count)



print("Drop collection")
#print(collection1.drop()) #return None

collections = db2.get_all_tables() #list of collections
print("Collections:" + ''.join(collections))

#table1.create()

#rows=[[2,2]]

#df=pd.DataFrame(rows,columns=["test2","test3"])

#table1.insert_from_df(df)

#rows=table1.select_all()


#table1.drop() ?

#db1.close_connection()