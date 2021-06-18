from datetime import datetime
import pymysql as msql

# print (datetime.now())

# try:
#     cnx = msql.connect(user='root', password='secret', host= '192.168.1.150', port='3306',  database='streamlit')
#     cursor = cnx.cursor()
# except msql.Error as err:
#   if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#     print("Something is wrong with your user name or password")
#   elif err.errno == errorcode.ER_BAD_DB_ERROR:
#     print("Database does not exist")
#   else:
#     print(err)
# else:
#   cnx.close()

row = [10, 25, 69, 20, 79, 31.4, 0.471, 100, str(datetime.now())]

print (str(tuple(row)))

cnx = msql.connect(user='root', password='secret', host= '192.168.1.150', port=3306, database = 'streamlit', autocommit=True)
cursor = cnx.cursor()

qry = """INSERT INTO streamlit.diabetes_app  (pregnancies, glucose, bloodpressure, skinthickness, insulin, bmi, dpf, age, create_datetime) VALUES """ + str(tuple(row)) +";"

print (qry)
cursor.execute(qry)
cnx.close()