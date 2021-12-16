from sqlalchemy import create_engine
# from sqlalchemy.types import Date
import sqlite3
import pyodbc


def mssql():
    dialect = "mssql+pyodbc://"
    ServerName_Port = "DESKTOP-ERMN8SE\INSTANCESKV:1433"
    UserName = "sa"
    Pwd = "Mechanical"
    Database = "rccp_tool"
    Driver = "SQL+Server+Native+Client+11.0"

    engine = create_engine(dialect+UserName + ':' + Pwd + '@' +ServerName_Port + '/' + Database + '?' + 'driver=' + Driver, fast_executemany = True)

    conn = engine.connect()
    return (conn, engine)
    # print (conn)

def pyodbc_mssql():
    server = 'DESKTOP-ERMN8SE\INSTANCESKV' 
    database = 'rccp_tool' 
    username = 'sa' 
    password = 'Mechanical' 
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password,
                        autocommit=True, fast_executemany=True)
    cursor = cnxn.cursor()
    return (cnxn,cursor)