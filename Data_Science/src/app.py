
# import the necessary libraries
import streamlit as st
import joblib
import pandas as pd
from PIL import Image
import os
from datetime import datetime
import pymysql as msql

path = os.getcwd()

@st.cache(allow_output_mutation=True)
def load(scaler_path, model_path):
    sc = joblib.load(scaler_path)
    model = joblib.load(model_path)
    return sc, model

def db_connection():
    cnx = msql.connect(user='root', password='secret', host= 'mysql', port=3306, database = 'streamlit', autocommit=True)
    cursor = cnx.cursor()
    return (cnx, cursor)

def inference(row, scaler, model, feat_cols):
    df = pd.DataFrame([row], columns=feat_cols)
    X = scaler.transform(df)
    features = pd.DataFrame(X, columns=feat_cols)
    if (model.predict(features) == 0):
        return "This is a healthy person!"
    else:
        return "This person has high chances of having diabetes!"


# Add streamlit title, add descriptions and load an attractive image
st.title('Diabetes Prediction App')
st.write('The data for the following example is originally from the National Institute of Diabetes and Digestive and Kidney Diseases and contains information on females at least 21 years old of Pima Indian heritage. This is a sample application and cannot be used as a substitute for real medical advice.')
# image = Image.open('data/diabetes_image.jpg')
# st.image(image, use_column_width=True)
st.write('Please fill in the details of the person under consideration in the left sidebar and click on the button below!')


age = st.sidebar.number_input("Age in Years", 1, 150, 25, 1)
pregnancies = st.sidebar.number_input("Number of Pregnancies", 0, 20, 0, 1)
glucose = st.sidebar.slider("Glucose Level", 0, 200, 25, 1)
skinthickness = st.sidebar.slider("Skin Thickness", 0, 99, 20, 1)
bloodpressure = st.sidebar.slider('Blood Pressure', 0, 122, 69, 1)
insulin = st.sidebar.slider("Insulin", 0, 846, 79, 1)
bmi = st.sidebar.slider("BMI", 0.0, 67.1, 31.4, 0.1)
dpf = st.sidebar.slider("Diabetes Pedigree Function", 0.000, 2.420, 0.471, 0.001)

row = [pregnancies, glucose, bloodpressure, skinthickness, insulin, bmi, dpf, age]

db_row = [pregnancies, glucose, bloodpressure, skinthickness, insulin, bmi, dpf, age, str(datetime.now())]

if __name__ == "__main__":
    
    if (st.button('Find Health Status')):
        feat_cols = ['Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness', 'Insulin', 'BMI', 'DiabetesPedigreeFunction', 'Age']
    
        sc, model = load(os.path.join(path, "resources", "scaler.joblib"),  os.path.join(path, "resources", "model.joblib"))
    
        result = inference(row, sc, model, feat_cols)

        # display the output (Step 4)
        st.write(result)


# --------DB Write---------------
        # with open(os.path.join(path, 'data', 'data.txt'), 'a') as fl:
        #     fl.write(str(row))
        #     fl.write("\n")
        #     print (row)
        qry = """INSERT INTO streamlit.diabetes_app (pregnancies, glucose, bloodpressure, skinthickness, insulin, bmi, dpf, age, create_datetime) VALUES """ + str(tuple(db_row)) +";"
        cnx, cursor = db_connection()
        cursor.execute(qry)
        cnx.close()

