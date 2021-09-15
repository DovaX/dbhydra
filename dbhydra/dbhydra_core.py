"""DB Hydra ORM"""
import pymongo
import pyodbc
import pandas as pd
import pymysql as MySQLdb
#import MySQLdb
import pickle

def read_file(file):
    """Reads txt file -> list"""

    with open(file,"r") as f:
        rows = f.readlines()
        for i,row in enumerate(rows):
            rows[i]=row.replace("\n","")
    return(rows)
    
def read_connection_details(config_file):
    connection_details=read_file(config_file)
    db_details={}
    for detail in connection_details:
        key=detail.split("=")[0]
        value=detail.split("=")[1]
        db_details[key]=value
    print(", ".join([db_details['DB_SERVER'],db_details['DB_DATABASE'],db_details['DB_USERNAME']]))
    return(db_details)

class AbstractDB:
    def __init__(self,config_file="config.ini",db_details=None):
        if db_details is None:    
            db_details=read_connection_details(config_file)
        locally=True
        if db_details["LOCALLY"]=="False":
            locally=False

        self.DB_SERVER=db_details["DB_SERVER"]
        self.DB_DATABASE=db_details["DB_DATABASE"]
        self.DB_USERNAME = db_details["DB_USERNAME"]
        self.DB_PASSWORD = db_details["DB_PASSWORD"]
        
        if "DB_DRIVER" in db_details.keys():
            self.DB_DRIVER = db_details["DB_DRIVER"]
        else:
            self.DB_DRIVER="ODBC Driver 13 for SQL Server"
        
        if locally:
            self.connect_locally()
        else:
            self.connect_remotely()
            
    def execute(self,query):
        self.cursor.execute(query)
        self.cursor.commit() 
        
    def close_connection(self):
        self.connection.close()
        print("DB connection closed")  


class AbstractDBNonSQL:
    def __init__(self, config_file="config-mongo.ini", db_details=None):
        if db_details is None:
            db_details = read_connection_details(config_file)
        locally = True
        if db_details["LOCALLY"] == "False":
            locally = False

        self.DB_SERVER = db_details["DB_SERVER"]
        self.DB_DATABASE = db_details["DB_DATABASE"]
        self.DB_USERNAME = db_details["DB_USERNAME"]
        self.DB_PASSWORD = db_details["DB_PASSWORD"]
        if locally:
            self.connect_locally()
        else:
            self.connect_remotely()

        #TODO, execute, close
class db(AbstractDB):
    def connect_remotely(self):
        
        self.connection = pyodbc.connect(
            r'DRIVER={'+self.DB_DRIVER+'};'
            r'SERVER=' + self.DB_SERVER + ';'
            r'DATABASE=' + self.DB_DATABASE + ';'
            r'UID=' + self.DB_USERNAME + ';'
            r'PWD=' + self.DB_PASSWORD + '',timeout=1
        )      
        self.cursor = self.connection.cursor()
        print("DB connection established")

    def connect_locally(self):
        self.connection = pyodbc.connect(
            r'DRIVER={'+self.DB_DRIVER+'};'
            r'SERVER=' + self.DB_SERVER + ';'
            r'DATABASE=' + self.DB_DATABASE + ';'
            r'TRUSTED_CONNECTION=yes;',timeout=1
            #r'PWD=' + self.DB_PASSWORD + '')
        )
        
        self.cursor = self.connection.cursor()
        print("DB connection established")
    
    def get_all_tables(self):
        sysobjects_table=Table(self, "sysobjects",["name"],["nvarchar(100)"])
        query="select name from sysobjects where xtype='U'"
        rows=sysobjects_table.select(query)
        return(rows)

    def generate_table_dict(self):        
        tables=self.get_all_tables()
        table_dict=dict()
        for i,table in enumerate(tables):
            table_dict[table]=Table.init_all_columns(self,table)

        return(table_dict)
    
    
    def get_foreign_keys_columns(self):
        sys_foreign_keys_columns_table=Table(self,"sys.foreign_key_columns",["parent_object_id","parent_column_id","referenced_object_id","referenced_column_id"],["int","int","int","int"])
        query="select parent_object_id,parent_column_id,referenced_object_id,referenced_column_id from sys.foreign_key_columns"
        rows=sys_foreign_keys_columns_table.select(query)
        
        sys_foreign_keys_columns_table=Table(self,"sys.tables",["object_id","name"],["int","nvarchar(100)"])
        query="select object_id,name from sys.tables"
        table_names=sys_foreign_keys_columns_table.select(query)
        
        table_id_name_dict={x[0]:x[1] for x in table_names}
        
        foreign_keys=[]
        for i,row in enumerate(rows):
            fk={"parent_table":table_id_name_dict[row[0]],"parent_column_id":row[1]-1,"referenced_table":table_id_name_dict[row[2]],"referenced_column_id":row[3]-1} #minus 1 because of indexing from 0  
            foreign_keys.append(fk)
        
        return(foreign_keys)
        
        
class Mysqldb(AbstractDB):           
    def connect_locally(self):
        self.connection = MySQLdb.connect(self.DB_SERVER,self.DB_USERNAME,self.DB_PASSWORD,self.DB_DATABASE)
        self.cursor = self.connection.cursor()
        print("DB connection established")
        
    def connect_remotely(self):
        self.connection = MySQLdb.connect(self.DB_SERVER,self.DB_USERNAME,self.DB_PASSWORD,self.DB_DATABASE)
        self.cursor = self.connection.cursor()
        print("DB connection established")
        
    def execute(self,query):
        self.cursor.execute(query)
        self.connection.commit() 
       
    def get_all_tables(self):
        sysobjects_table=Table(self, "information_schema.tables",["TABLE_NAME"],["nvarchar(100)"])
        query="SELECT TABLE_NAME,TABLE_TYPE,TABLE_SCHEMA FROM information_schema.tables where TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='"+self.DB_DATABASE+"' ;"
        rows=sysobjects_table.select(query)
        tables=[x[0] for x in rows]
        return(tables) 
       
    def generate_table_dict(self):        
        tables=self.get_all_tables()
        table_dict=dict()
        for i,table in enumerate(tables):
            table_dict[table]=MysqlTable.init_all_columns(self,table)
        return(table_dict)

class MyNonSqlDb(AbstractDBNonSQL):
    def connect_remotely(self):
        #self.connection = MySQLdb.connect(self.DB_SERVER, self.DB_USERNAME, self.DB_PASSWORD, self.DB_DATABASE)
        self.connection = pymongo.MongoClient("mongodb+srv://" + self.DB_USERNAME + ":" + self.DB_PASSWORD +"@" + self.DB_SERVER + "/" + self.DB_DATABASE + "?retryWrites=true&w=majority")
        print(self.connection.list_database_names())
        self.database = self.connection[self.DB_DATABASE]
        print(self.database)
        print("DB connection established")

    def execute(self, query):
        self.cursor.execute(query)
        self.connection.commit()

    def get_all_collections(self):
        return self.database.list_collection_names()
    def createCollection(self, name):
        return self.database[name]


#Tables 
class AbstractSelectable:
    def __init__(self,db1,name,columns=None):  
        self.db1=db1
        self.name=name
        self.columns=columns
    
    def select(self,query):
        """given SELECT query returns Python list"""
        """Columns give the number of selected columns"""
        self.db1.cursor.execute(query)
        column_string=query.lower().split("from")[0]
        if "*" in column_string:
            columns=len(self.columns)
        elif column_string.find(",") == -1:
            columns = 1
        else:
            columns = len(column_string.split(","))
        rows = self.db1.cursor.fetchall()
        if columns==1:
            cleared_rows_list = [item[0] for item in rows]
        
        if columns>1:
            cleared_rows_list=[]
            for row in rows: #Because of unhashable type: 'pyodbc.Row'
                list1=[]
                for i in range(columns):
                    #print(row)
                    list1.append(row[i])
                cleared_rows_list.append(list1)  
        return(cleared_rows_list)
     
    def select_all(self):
        list1=self.select("SELECT * FROM "+self.name)
        return(list1)

    def select_to_df(self):
        rows=self.select_all()
        table_columns=self.columns
        demands_df=pd.DataFrame(rows,columns=table_columns)
        return(demands_df)    
    
    def export_to_xlsx(self):
        list1=self.select_all()
        df1=pd.DataFrame(list1,columns=["id"]+self.columns)
        df1.to_excel("items.xlsx")
        
class Selectable(AbstractSelectable): #Tables, views, and results of joins
    pass

class MysqlSelectable(AbstractSelectable):      
    def select(self,query):
        """TODO"""
        print(query)
        self.db1.execute(query)
        rows = self.db1.cursor.fetchall()
        return(rows)
    
class AbstractJoinable(AbstractSelectable):
    def __init__(self,db1,name,columns=None):
        super().__init__(db1,name,columns)
    
    def inner_join(self,joinable,column1,column2):
        join_name=self.name+" INNER JOIN "+joinable.name+" ON "+column1+"="+column2
        join_columns=list(set(self.columns) | set(joinable.columns))
        new_joinable=Joinable(self.db1,join_name,join_columns)
        return(new_joinable)
 
class Joinable(Selectable):
    pass

class AbstractTable(AbstractJoinable):
    def __init__(self,db1,name,columns=None,types=None):
        super().__init__(db1,name,columns)
        self.types=types

    def drop(self):
        query="DROP TABLE "+self.name
        print(query)
        self.db1.execute(query)
        
    def update(self,variable_assign,where=None):
        if where is None:
            query = "UPDATE "+self.name+" SET "+variable_assign
        else:
            query = "UPDATE "+self.name+" SET "+variable_assign+" WHERE "+where
        print(query)
        self.db1.execute(query)    

    def insert_from_df(self,df,batch=1,try_mode=False):
        assert len(df.columns)+1==len(self.columns) #+1 because of id column
        
        #handling nan values -> change to NULL TODO
        for column in list(df.columns):
            df.loc[pd.isna(df[column]), column] = "NULL"
        
        rows=df.values.tolist()
        self.insert(rows,batch=batch,try_mode=try_mode)

class Collection():
    def __init__(self, db, name):
        self.name = name
        self.db = db
        self.collection = self.db.createCollection(name)
    def drop(self):
        return self.collection.drop()
    def insert(self, document):
        return self.collection.insert_one(document)
    def insertMore(self, documents):
        return self.collection.insert_many(documents)

    def find(self, query, columns={}):

        if(len(columns) == 0):
            return self.collection.find(query)
        else:
            return self.collection.find(query, columns)
    def findSort(self, query, fieldname, direction, columns={}):
        if (len(columns) == 0):
            return self.collection.find(query).sort(fieldname, direction)
        else:
            return self.collection.find(query, columns).sort(fieldname, direction)
    def delete(self, query={}):
        return self.collection.delete_many(query)
    def update(self, query, newvalues):
        return self.collection.update(query, newvalues)
    def insertFromDataFrame(self, dataframe):
        return self.collection.insert_many(dataframe.to_dict)
class Table(Joinable,AbstractTable):
    def __init__(self,db1,name,columns=None,types=None):
        """Override joinable init"""
        super().__init__(db1,name,columns)
        self.types=types
        
        
    @classmethod
    def init_all_columns(cls,db1,name):
        temporary_table=cls(db1,name)
        columns=temporary_table.get_all_columns()
        types=temporary_table.get_all_types()
        return(cls(db1,name,columns,types))
        
    def get_all_columns(self):
        information_schema_table=Table(self.db1,'INFORMATION_SCHEMA.COLUMNS')
        query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '"+self.name+"'"
        columns=information_schema_table.select(query)
        return(columns)

    def get_all_types(self):
        information_schema_table=Table(self.db1,'INFORMATION_SCHEMA.COLUMNS',['DATA_TYPE'],['nvarchar(50)'])
        query="SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '"+self.name+"'"
        types=information_schema_table.select(query)
        return(types)


    def create(self):
        assert len(self.columns)==len(self.types)
        assert self.columns[0]=="id"
        assert self.types[0]=="int"
        query="CREATE TABLE "+self.name+"(id INT IDENTITY(1,1) NOT NULL,"
        for i in range(1,len(self.columns)):
            query+=self.columns[i]+" "+self.types[i]+","
        query+="PRIMARY KEY(id))"        
        print(query)
        try:
            self.db1.execute(query)
        except Exception as e:
            print("Table "+self.name+" already exists:",e)
            print("Check the specification of table columns and their types")
        
    def insert(self,rows,batch=1,replace_apostrophes=True,try_mode=False):
        
        assert len(self.columns)==len(self.types)
        for k in range(len(rows)):
            if k%batch==0:
                query="INSERT INTO "+self.name+" ("
                for i in range(1,len(self.columns)):
                    if i<len(rows[k])+1:
                        query+=self.columns[i]+","
                if len(rows)<len(self.columns):
                    print(len(self.columns)-len(rows),"columns were not specified")
                query=query[:-1]+") VALUES "
            
            query+="("
            for j in range(len(rows[k])):
                if rows[k][j]=="NULL" or rows[k][j]==None or rows[k][j]=="None": #NaN hodnoty
                    query+="NULL,"
                elif "nvarchar" in self.types[j+1]:
                    if replace_apostrophes:
                        rows[k][j]=str(rows[k][j]).replace("'","")
                    query+="N'"+str(rows[k][j])+"',"
                elif "varchar" in self.types[j+1]:
                    if replace_apostrophes:
                        rows[k][j]=str(rows[k][j]).replace("'","")
                    query+="'"+str(rows[k][j])+"',"
                elif self.types[j+1]=="int":
                    query+=str(rows[k][j])+","
                elif "date" in self.types[j+1]:
                    query+="'"+str(rows[k][j])+"',"
                elif "datetime" in self.types[j+1]:
                    query+="'"+str(rows[k][j])+"',"
                else:
                    query+=str(rows[k][j])+","

            query=query[:-1]+"),"            
            if k%batch==batch-1 or k==len(rows)-1:
                query=query[:-1]
                print(query)
                if not try_mode:
                    self.db1.execute(query) 
                else:
                    try:
                        self.db1.execute(query)  
                    except Exception as e:
                        file=open("log.txt","a")
                        print("Query",query,"Could not be inserted:",e)
                        file.write("Query "+str(query)+" could not be inserted:"+str(e)+"\n")
                        file.close()
                            
    def delete(self,where=None):
        if where is None:
            query = "DELETE FROM "+self.name
        else:
            query = "DELETE FROM "+self.name+" WHERE "+where
        print(query)
        self.db1.execute(query)
        
        
    
    def get_foreign_keys_for_table(self,table_dict,foreign_keys):
        #table_dict is in format from db function: generate_table_dict()
        #foreign_keys are in format from db function: get_foreign_keys_columns()
        parent_foreign_keys=[]
        for i,fk in enumerate(foreign_keys):
            if fk["parent_table"]==self.name:
                try:
                    print(fk["parent_column_id"])
                    print(self.columns)
                    print(self.name)
                    fk["parent_column_name"]=self.columns[fk["parent_column_id"]]
                    fk["referenced_column_name"]=table_dict[fk["referenced_table"]].columns[fk["referenced_column_id"]]
                    parent_foreign_keys.append(fk)
                except IndexError as e:
                    print("Warning: IndexError for foreign key self.columns[fk[parent_column_id]]:",e)
        return(parent_foreign_keys)
    
                
       
class MysqlTable(MysqlSelectable,AbstractTable):
    def __init__(self,db1,name,columns=None,types=None):
        super().__init__(db1,name,columns)
        self.types=types
     
        
    def get_all_columns(self):
        information_schema_table=Table(self.db1,'INFORMATION_SCHEMA.COLUMNS')
        query="SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '"+self.name+"'"
        columns=information_schema_table.select(query)
        return(columns)

    def get_all_types(self):
        information_schema_table=Table(self.db1,'INFORMATION_SCHEMA.COLUMNS',['DATA_TYPE'],['nvarchar(50)'])
        query="SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME  = '"+self.name+"'"
        types=information_schema_table.select(query)
        return(types)
        
     
    @classmethod
    def init_all_columns(cls,db1,name):
        temporary_table=cls(db1,name)
        columns=temporary_table.get_all_columns()
        types=temporary_table.get_all_types()
        return(cls(db1,name,columns,types))
        
        
    def create(self,foreign_keys=None):
        assert len(self.columns)==len(self.types)
        assert self.columns[0]=="id"
        assert self.types[0]=="int"
        query="CREATE TABLE "+self.name+"(id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
        for i in range(1,len(self.columns)):
            query+=self.columns[i]+" "+self.types[i]+","

        query=query[:-1]
        query+=")"
        print(query)
        try:
            self.db1.execute(query)
        except Exception as e:
            print("Table "+self.name+" already exists:",e)
            print("Check the specification of table columns and their types")
                            
    def insert(self,rows,batch=1,replace_apostrophes=True,try_mode=False):
        
        assert len(self.columns)==len(self.types)
        for k in range(len(rows)):
            if k%batch==0:
                query="INSERT INTO "+self.name+" ("
                for i in range(1,len(self.columns)):
                    if i<len(rows[k])+1:
                        query+=self.columns[i]+","
                if len(rows)<len(self.columns):
                    print(len(self.columns)-len(rows),"columns were not specified")
                query=query[:-1]+") VALUES "
            
            query+="("
            for j in range(len(rows[k])):
                if rows[k][j]=="NULL" or rows[k][j]==None or rows[k][j]=="None": #NaN hodnoty
                    query+="NULL,"
                elif "nvarchar" in self.types[j+1]:
                    if replace_apostrophes:
                        rows[k][j]=str(rows[k][j]).replace("'","")
                    query+="N'"+str(rows[k][j])+"',"
                elif "varchar" in self.types[j+1]:
                    if replace_apostrophes:
                        rows[k][j]=str(rows[k][j]).replace("'","")
                    query+="'"+str(rows[k][j])+"',"
                elif self.types[j+1]=="int":
                    query+=str(rows[k][j])+","
                elif "date" in self.types[j+1]:
                    query+="'"+str(rows[k][j])+"',"
                elif "datetime" in self.types[j+1]:
                    query+="'"+str(rows[k][j])+"',"
                else:
                    query+=str(rows[k][j])+","

            query=query[:-1]+"),"            
            if k%batch==batch-1 or k==len(rows)-1:
                query=query[:-1]
                print(query)
                if not try_mode:
                    self.db1.execute(query) 
                else:
                    try:
                        self.db1.execute(query)  
                    except Exception as e:
                        file=open("log.txt","a")
                        print("Query",query,"Could not be inserted:",e)
                        file.write("Query "+str(query)+" could not be inserted:"+str(e)+"\n")
                        file.close()

    def add_foreign_key(self,foreign_key):
        parent_id=foreign_key['parent_id']
        parent=foreign_key['parent']
        query="ALTER TABLE "+self.name+" MODIFY "+parent_id+" INT UNSIGNED"            
        print(query)
        self.db1.execute(query)
        query="ALTER TABLE "+self.name+" ADD FOREIGN KEY ("+parent_id+") REFERENCES "+parent+"(id)"
        print(query)
        self.db1.execute(query)


class XlsxDB:
    def __init__(self,name="new_db",config_file="config.ini"):   
        self.name=name
        
        """
        db_details=read_connection_details(config_file)
        locally=True
        if db_details["LOCALLY"]=="False":
            locally=False
            
        if locally:
            self.DB_SERVER=db_details["DB_SERVER"]
            self.DB_DATABASE=db_details["DB_DATABASE"]
            self.DB_USERNAME = db_details["DB_USERNAME"]
            self.DB_PASSWORD = db_details["DB_PASSWORD"]
            self.connect_locally()
        else:
            self.DB_SERVER = db_details["DB_SERVER"]
            self.DB_DATABASE = db_details["DB_DATABASE"]
            self.DB_USERNAME = db_details["DB_USERNAME"]
            self.DB_PASSWORD = db_details["DB_PASSWORD"]
            self.connect_remotely()
        """
            
    def execute(self,query):
        pass
        #self.cursor.execute(query)
        #self.cursor.commit() 
        
    def close_connection(self):
        pass
        #self.connection.close()
        #print("DB connection closed")  
        
        
    def create_database(self):
        import os
        try:
            os.mkdir(self.name)
            print("Database created")
        except:
            print("Database is already created")
 
        
class XlsxTable(AbstractTable):
    def __init__(self,db1,name,columns=None,types=None):
        super().__init__(db1,name,columns)
        self.types=types
        
    def select_to_df(self):
        try:
            df=pd.read_excel(self.db1.name+"//"+self.name+".xlsx")
            #cols=df.columns
            #print(cols)
            #df.set_index(cols[0],inplace=True)
            #df.drop(df.columns[0],axis=1,inplace=True)
        
        except Exception as e:
            print("Error: ",e)
            df=pd.DataFrame(columns=self.columns)
        return(df)      
    
    
    def insert_from_df(self,df,batch=1,try_mode=False):
        assert len(df.columns)+1==len(self.columns) #+1 because of id column
        
        original_df=self.select_to_df()
        try:
            original_df=original_df.drop(original_df.columns[0],axis=1)
        except:
            print("First column could not be dropped")
        
        df.columns=original_df.columns
        #handling nan values -> change to NULL TODO
        for column in list(df.columns):
            df.loc[pd.isna(df[column]), column] = "NULL"

        def concat_with_reset_index_in_second_df(original_df,df):
            """Subsitute of reset_index method because we need to maintain the ids of original df"""
            maximal_index=max(original_df.index)
            df.index=df.index+maximal_index+1
            original_df=original_df.append(df)
            #df=pd.concat([original_df,df])
            return(original_df)
            
        if len(original_df)>0:
            df=concat_with_reset_index_in_second_df(original_df,df)
        
        df.to_excel(self.db1.name+"\\"+self.name+".xlsx")
    
    def replace_from_df(self,df):
        assert len(df.columns)==len(self.columns) #+1 because of id column
        df.drop(df.columns[0],axis=1,inplace=True)
        df.to_excel(self.db1.name+"\\"+self.name+".xlsx")  
    
    def update(self,variable_assign,where=None):
        def split_assign(variable_assign):
            variable=variable_assign.split("=")[0]
            value=variable_assign.split("=")[1]
            try:
                value=int(value) #integers
            except:
                value=value.split("'")[1] #strings
            return(variable,value)
        
        variable,value=split_assign(variable_assign)
        df=self.select_to_df()
        print(where)
        print(variable,value)
        if where is None:    
            df[variable]=value  
            print(df)
        else:    
            where_variable,where_value=split_assign(where)
            df[variable] = df[where_variable].apply(lambda x: value if x==where_value else x)
        self.replace_from_df(df)

    def delete(self,where=None):    
        def split_assign(variable_assign):
            variable=variable_assign.split("=")[0]
            value=variable_assign.split("=")[1]
            try:
                value=int(value) #integers
            except:
                value=value.split("'")[1] #strings
            return(variable,value)
       
        df=self.select_to_df()
        if where is None:    
            df=df.iloc[0:0]
            print(df)
        else:    
            where_variable,where_value=split_assign(where)
            df.drop(df[df[where_variable]==where_value].index, inplace=True)
        self.replace_from_df(df)
        
#dataframe - dictionary auxiliary functions     
def df_to_dict(df,column1,column2):
    dictionary=df.set_index(column1).to_dict()[column2]
    return(dictionary)

def dict_to_df(dictionary,column1,column2):
    df=pd.DataFrame(list(dictionary.items()), columns=[column1, column2])
    return(df)
