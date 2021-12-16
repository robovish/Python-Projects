import pandas as pd

from db_connection import mssql, pyodbc_mssql
import query

cnx, cursor = pyodbc_mssql()

cursor.execute(query.inv_temp1())

cursor.close()
cnx.close()