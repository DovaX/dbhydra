import pandas as pd
from typing import Optional, Any
import numpy as np
import abc


class Query:
    def __init__(self, query):
        pass
        
        
    
    
    


class Select:
    def __init__(self, columns:list, table):
        self.columns=columns
        self.table=table
        
    def __str__(self):
        return("SELECT "+",".join(self.columns)+" FROM "+self.table.name)
    
    
class Insert:
    def __init__(self,column_value_dict:dict, table):
        self.column_value_dict=column_value_dict
        self.table=table
        
        
class Update:
    def __init__(self,column_value_dict:dict, table):
        self.column_value_dict=column_value_dict
        self.table=table
        
        
class Delete:
    def __init__(self, table):
        self.table=table
        
class Where:
    def __init__(self, column: str, value: Any, operator="="):
        self.column = column
        self.value = value
        self.operator = operator
    
    def __str__(self):
        return f"WHERE {self.column} {self.operator} {self.value}"
      
  

# Tables
class AbstractSelectable:
    def __init__(self, db1, name, columns=None):
        self.db1 = db1
        self.name = name
        self.columns = columns
        self.query_building_enabled=False
        self._query_blocks=[]
        

    def select(self, query, flatten_results=False):
        """given SELECT query returns Python list"""
        """Columns give the number of selected columns"""
        
        
        def get_selected_columns(query):
            column_string = query.lower().split("from")[0]
            if "*" in column_string:
                columns = self.columns
            elif column_string.find(",") == -1:
                columns = [column_string.replace("select","").strip()]
            else:
                columns = [x.strip() for x in column_string.split(",")]
            return(columns)
        
        
        if self.query_building_enabled:
            columns=get_selected_columns(query)
            self._query_blocks.append(Select(columns, self))
            return(self) #to enable chaining
        
        else:
            print(query)
            self.db1.cursor.execute(query)
            rows = self.db1.cursor.fetchall()
               
            
            def cast_rows_to_list_of_lists(rows):
                cleared_rows = []
                for row in rows:  # Because of unhashable type: 'pyodbc.Row'
                    list1 = []
                    for i in range(columns_count):
                        list1.append(row[i])
                    cleared_rows.append(list1)
                return(cleared_rows)
            
            def flatten_results(rows, columns_count):
                """Returns flat list for one column and list of lists for multiple columns"""
                
                    
                if columns_count == 1:
                    cleared_rows = [item[0] for item in rows]
                
                return(cleared_rows)
            
            rows=cast_rows_to_list_of_lists(rows)
            if flatten_results:
                columns_count=len(get_selected_columns(query))
                results=flatten_results(rows, columns_count)
            return (results)

    def select_all(self):
        all_cols_query = ""
        for col in self.columns:
            all_cols_query = all_cols_query + col + ","
        if all_cols_query[-1] == ",":
            all_cols_query = all_cols_query[:-1]
        list1 = self.select(f"SELECT {all_cols_query} FROM " + self.name)
        return (list1)

    def select_to_df(self):
        rows = self.select_all()
        table_columns = self.columns
        df = pd.DataFrame(rows, columns=table_columns)
        return (df)

    def export_to_xlsx(self, filename="items.xlsx"):
        list1 = self.select_all()
        df1 = pd.DataFrame(list1, columns=["id"] + self.columns)
        df1.to_excel(filename)
        
    def where(self, column, value, operator="="):
        self._query_blocks.append(Where(column, value, operator))
        return(self) #to enable chaining
        
    
    def _construct_query_string_from_blocks(self,blocks):
        core_query=" ".join([str(x) for x in blocks if type(x)!=Where])
        where_query=" ".join([str(x) for x in blocks if type(x)==Where])
        where_query=where_query.replace("WHERE","AND")
        where_query=where_query.replace("AND","WHERE",1) #First WHERE is not replaced by AND
        query=" ".join([core_query,where_query])
        print(query)
        return(query)
    
    def _build_queries_from_blocks(self):
        queries=[]
        grouped_blocks=[]
        for i,block in enumerate(self._query_blocks):
            if type(block)!=Where:
                grouped_blocks.append([block])
            else:
                grouped_blocks[-1].append(block)
            
        for i, grouped_block in enumerate(grouped_blocks):
            query=self._construct_query_string_from_blocks(grouped_block)
            queries.append(query)
            single_query_blocks=[block]
            
        return(queries)
    
    def execute(self, query=""):
        """passes execution to DB object"""
        if not self.query_building_enabled:
            self.db1.execute(query)
            
        else:
            queries=self._build_queries_from_blocks()
            for query in queries:
                self.db1.execute()
            self._query_blocks=[]


class AbstractJoinable(AbstractSelectable):
    def __init__(self, db1, name, columns=None):
        super().__init__(db1, name, columns)

    def inner_join(self, joinable, column1, column2):
        join_name = self.name + " INNER JOIN " + joinable.name + " ON " + column1 + "=" + column2
        join_columns = list(set(self.columns) | set(joinable.columns))
        new_joinable = AbstractJoinable(self.db1, join_name, join_columns)
        return (new_joinable)



class AbstractTable(AbstractJoinable, abc.ABC):
    def __init__(self, db1, name, columns = None, types: Optional[list[str]] = None, id_column_name: str = "id"):
        """id_column_name -> name of predefined column with autoincrement + PK"""
        super().__init__(db1, name, columns)
        self.types: list[str] = types #[] if types is None else types - is wrong, if types not initialized it should be None, not empty list
        self.id_column_name: str = id_column_name

    # Temporary disabled, please make sure this is implemented where needed, don't introduce breaking changes please
    # @abc.abstractmethod
    @classmethod
    def init_from_column_type_dict(cls, db1, name, column_type_dict, id_column_name="id"):
        column_converted_type_dict = db1._convert_column_type_dict_from_python(column_type_dict)
        columns = list(column_converted_type_dict.keys())
        types = list(column_converted_type_dict.values())
        return cls(db1, name, columns, types, id_column_name=id_column_name)
    
    
    def drop(self):
        query = "DROP TABLE " + self.name
        print(query)
        self.execute(query)

    def update(self, variable_assign, where=None):
        if where is None:
            query = "UPDATE " + self.name + " SET " + variable_assign
        else:
            query = "UPDATE " + self.name + " SET " + variable_assign + " WHERE " + where
        print(query)
        return self.execute(query)

    def update_from_df(
            self, update_df: pd.DataFrame, where_column: Optional[str] = None, where_value: Any = None) -> None:
        """Build UPDATE SQL query from a dataframe and execute it.

        :param update_df: Dataframe with updated values - MUST only hold a single row
        :type update_df: pd.DataFrame
        :param where_column: Column name to use for WHERE clause.
        :type where_column: str
        :param where_value: Value to use for WHERE clause.
        :type where_value: Any
        """
        if not len(update_df) == 1:
            raise ValueError("There can only be one row in the UPDATE dataframe")

        types_without_id_column = [type_ for column, type_ in zip(self.columns, self.types) 
                           if column != self.id_column_name]
        if len(types_without_id_column) != len(update_df.columns):
            raise AttributeError(
                "Number of columns in dataframe does not match number of columns in table"
            )

        column_value_string = ""
        for (column, cell_value), column_type in zip(update_df.iloc[0].items(), types_without_id_column):
            if cell_value is None:
                column_value_string += f"{column} = NULL, "
            elif column_type in ["double", "int", "tinyint"]:
                column_value_string += f"{column} = {cell_value}, "
            elif "varchar" in column_type:
                column_value_string += f"{column} = '{cell_value}', "
            elif column_type in ["json", "text", "mediumtext", "longtext", "datetime"]:
                column_value_string += f"{column} = '{cell_value}', "
            else:
                raise AttributeError(f"Unknown column type '{column_type}'")

        column_value_string = column_value_string.rstrip(", ")
        sql_query = f"UPDATE {self.name} SET {column_value_string}"

        if where_column is not None and where_value is not None:
            sql_query += f" WHERE {where_column} = {where_value};"
        else:
            sql_query += ";"

        print(sql_query)
        self.execute(sql_query)

    def _adjust_df(self, df: pd.DataFrame, debug_mode=False) -> pd.DataFrame:
        """
        Adjust DataFrame in case number of its columns differs from number of columns in DB table.
        :param df: original DF
        :param debug_mode: in debug mode a warning will be printed to console
        :return: adjusted DF
        """

        df_copy = df.copy()

        difference_columns_missing = list(set(self.columns).difference(set(df_copy.columns)).difference({'id'}))

        if difference_columns_missing:
            if debug_mode:
                print(f'DataFrame missing columns {difference_columns_missing} will be added as empty.')

            for col in difference_columns_missing:
                df_copy[col] = np.nan

        difference_columns_extra = list(set(df_copy.columns).difference(set(self.columns)))

        if difference_columns_extra:
            if debug_mode:
                print(f'DataFrame extra columns {difference_columns_extra} will be removed.')

            df_copy.drop(difference_columns_extra, axis=1, inplace=True)

        # Ensure the same order of columns in DF as in DB table
        cols_order = [x for x in self.columns if x != 'id']
        df_copy = df_copy[cols_order]

        return df_copy


    def insert_from_df(self, df, batch=1, try_mode=False, debug_mode=False, adjust_df=False, insert_id=False):

        if adjust_df:
            df = self._adjust_df(df, debug_mode)

        if insert_id:
            assert len(df.columns) == len(self.columns)
            assert set(df.columns) == set(self.columns)
            
            inserted_columns=self.columns
            
        else:
            assert len(df.columns) + 1 == len(self.columns) # +1 because of id column
            
            inserted_columns=list(dict.fromkeys(self.columns)) #DEDUPLICATION preserving order -> better than inserted_columns = set(self.columns) 
            id_index=inserted_columns.index(self.id_column_name)
            inserted_columns.pop(id_index)
            print(inserted_columns,df.columns)
            
            assert set(df.columns) == set(inserted_columns) #elements are matchin
            #df = df[inserted_columns]
        df = df[inserted_columns]

        pd_nullable_dtypes = {pd.Int8Dtype(), pd.Int16Dtype(), pd.Int32Dtype(), pd.Int64Dtype(),
                              pd.UInt8Dtype(), pd.UInt16Dtype(), pd.UInt32Dtype(), pd.UInt64Dtype(),
                              pd.Float32Dtype(), pd.Float64Dtype()}
        pd_nullable_columns = df.dtypes.isin(pd_nullable_dtypes)

        # cast pd nullable columns to float - changing to 'NULL' then proceeds without an error.
        # in database floats are again casted to integers where required
        df = df.copy()
        df.loc[:, pd_nullable_columns] = df.loc[:, pd_nullable_columns].astype(float)

        # TODO: handling nan values -> change to NULL
        for column in list(df.columns):
            df.loc[pd.isna(df[column]), column] = "NULL"

        # rows = df.values.tolist()
        # for i, row in enumerate(rows):
        #     for j, record in enumerate(row):
        #         if type(record) == str:
        #             rows[i][j] = "'" + record + "'"
        #print(rows)
        rows = df.values.tolist()
        return self.insert(rows, batch=batch, try_mode=try_mode, debug_mode=False, insert_id=insert_id)

    #TODO: need to solve inserting in different column_order
    #check df column names, permute if needed
    def insert_from_column_value_dict(self, dict, insert_id=False):
        df = pd.DataFrame(dict, index=[0])
        return self.insert_from_df(df, insert_id=insert_id)

    def insert_from_column_value_dict_list(self, list, insert_id=False):
        df = pd.DataFrame(list)
        self.insert_from_df(df, insert_id=insert_id)


    def delete(self, where=None):

        if where is None:
            query = "DELETE FROM " + self.name
        else:
            query = "DELETE FROM " + self.name + " WHERE " + where
        return self.execute(query)






table1=AbstractTable(None,"prices")
table1.query_building_enabled=True
table1.select("price")
table1.select("price, name").where("price",5).where("name","John")
table1.execute()

print(table1._query_blocks)


# select_query=price_table.select("price").where("id",1)
# update_query=table1.update().where()
# delete_query=table1.





# table1.select(columns).where("uid",10).where("project_uid",20)

#with db1.transaction() as t:
#    table1.select()
    
    