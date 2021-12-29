.. _introduction:

================================================
Introduction 
================================================

What is dbhydra?
==========================
Data science friendly ORM  (Object Relational Mapping) library combining Python, Pandas, and various SQL dialects.


Why dbhydra?
=============

Dbhydra package has been developed as an answer to non-existing easy-to-use database connection library with ORM (Object Relational Mapping) 
that would sufficiently fulfill needs of a data scientists, data engineers, and backend developers at the same time - especially:

* Simplicity of SQL queries
* Standardized approach to various DB dialects
* Object relational mapping with intuitive usage
* No need to specify detailed database models for existing DBs
* Easy creation of new DBs
* Fast and efficient DB migrations
* Flexibility and tailored methods to efficiently work with DataFrame objects (pandas)


Current Scope
==================================================

* Overall aims: Easy integration with Pandas, SQL SERVER/MySQL database, and exports/imports to/from excel/CSV format
* Done: Table functions (Create, Drop, Select, Update, Insert, and Delete) should be working fine
* Todo: Group by, Order by, Where, Linking of FK, Customizable PK,...

Contributing
==================================================

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.



Example of usage
===================================================


.. code-block:: python

    import dbhydra.dbhydra_core as dh
    db1=dh.db()
    
    table1 = dh.Table(db1,"test",["test1","test2","test3","test4"],["int","int","int","int"])
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
    
    table_test=dh.Table.init_all_columns(db1,"test")
    
    print(table_test.columns)
       
    table2 = dh.Table(db1,"test_new",["id","test2"],["int","nvarchar(20)"])
    #table2.create()
    
 
     