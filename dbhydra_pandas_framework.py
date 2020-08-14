import dbhydra as dh
import pandas as pd

def init_dorm_tables(db1,list_of_table_names,dorm_tables={}):
    for table_name in list_of_table_names:
        dorm_tables[table_name]=dh.Table.init_all_columns(db1,table_name)  
    return(dorm_tables)

def init_dataframes_from_tables(db1,dorm_tables,list_of_table_names,dfs={}):  
    for table_name in list_of_table_names:
        dfs[table_name]=dorm_tables[table_name].select_to_df()
    return(dfs)

def init_dicts_between_dataframes(dfs,dictionary_definitions,dicts={}):
    #input list of lists of 4 strings: df_name,column1,column2,dict_name
    dicts={}
    for definition in dictionary_definitions:
        dicts[definition[3]]=dh.df_to_dict(dfs[definition[0]],definition[1],definition[2])  
    return(dicts)