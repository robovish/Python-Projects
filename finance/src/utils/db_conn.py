import toml
from sqlalchemy import create_engine
from sqlalchemy.types import Date
import pyodbc

config = toml.load(r'D:\Python-Projects\finance\config.toml')

def db_conn():
    """ -----------Connection string for MS-SQL--------------"""

    dialect = config['DATABASE']['DIALECT']
    server = config['DATABASE']['SERVER'] + '\\' +  config['DATABASE']['INSTANCE']
    ServerName_Port = config['DATABASE']['SERVER'] + '\\' + config['DATABASE']['INSTANCE'] + ':' + config['DATABASE']['PORT']
    database =  config['DATABASE']['DB_NAME'] 
    username =  config['DATABASE']['USER']
    password =  config['DATABASE']['PASSWORD']
    driver = config['DATABASE']['DRIVER']
    autocommit = config['DATABASE']['AUTOCOMMIT']
    fast_executemany = config['DATABASE']['FAST_EXECUTE']

    mssql_engine = create_engine(dialect+username + ':' + password + '@' +ServerName_Port + '/' + database + '?' + 'driver=' + driver)

    cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password,
                        autocommit=autocommit, fast_executemany=fast_executemany)
    cursor = cnxn.cursor()
    return (mssql_engine, cnxn, cursor)
