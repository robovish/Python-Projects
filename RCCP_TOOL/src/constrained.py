import pandas as pd

from db_connection import mssql, pyodbc_mssql
import query

cnx, cursor = pyodbc_mssql()

cursor.execute(query.sku_final_list())

cursor.close()
cnx.close()

# print (query.sku_final_list())