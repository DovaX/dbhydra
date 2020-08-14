import dogui.dogui_core as dg
import dbhydra as dh

db1=dh.Mysqldb("config.ini")

def gui_create_table():
    name=entry1.text.get()
    columns=entry2.text.get().split(",")
    types=entry3.text.get().split(",")
    table1=dh.MysqlTable(db1,name,columns,types)
    table1.create()
    
def gui_drop_table():
    name=entry1.text.get()
    table1=dh.MysqlTable(db1,name)
    table1.drop()
    

def gui_add_foreign_key():
    name=entry4.text.get()
    parent_table=entry5.text.get()
    parent_id=entry6.text.get()
    table1=dh.MysqlTable(db1,name)
    
    if parent_table!="" and parent_id!="":
        foreign_key={'parent':parent_table,'parent_id':parent_id}
        table1.add_foreign_key(foreign_key)

def gui_execute_query():
    query=entry7.text.get()
    db1.execute(query)
    
    
gui1=dg.GUI(title="DB Hydra Builder GUI")
dg.Label(gui1.window,"Name",1,1)
dg.Label(gui1.window,"Columns",1,2)
dg.Label(gui1.window,"Types",1,3)

entry1=dg.Entry(gui1.window,2,1,width=15)
entry2=dg.Entry(gui1.window,2,2,width=50)
entry3=dg.Entry(gui1.window,2,3,width=50)
dg.Button(gui1.window,"Create Table",gui_create_table,3,1)
dg.Button(gui1.window,"Drop Table",gui_drop_table,3,2)

dg.Label(gui1.window,"FK - table",4,1)
dg.Label(gui1.window,"FK - parent table",4,2)
dg.Label(gui1.window,"FK - parent_id",4,3)

entry4=dg.Entry(gui1.window,5,1,width=15)
entry5=dg.Entry(gui1.window,5,2,width=15)
entry6=dg.Entry(gui1.window,5,3,width=15)
dg.Button(gui1.window,"Add foreign key",gui_add_foreign_key,6,1)

entry7=dg.Entry(gui1.window,7,1,width=15)
dg.Button(gui1.window,"Execute",gui_execute_query,7,2)

gui1.build_gui()