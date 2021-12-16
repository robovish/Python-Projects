import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from sqlalchemy.types import Date
import streamlit as st
import datetime as dt
import time
import os

# Add streamlit title, add descriptions and load an attractive image
st.title('IFIT SUPPLY CHAIN TOOLS')
st.write('This portal contains Supply Chain Tools')
# image = Image.open('data/diabetes_image.jpg')
# st.image(image, use_column_width=True)
# st.write('Please fill in the details of the person under consideration in the left sidebar and click on the button below!')


tool_name = st.sidebar.selectbox('Tool Name', ['Select','RCCP Tool', 'Forecasting Tool' ])

if __name__ == "__main__":
    
    if (st.sidebar.button('Run')):
        if (tool_name == 'RCCP Tool'):
            " Write code using subprocess to call rccp executable"
            st.write('You selected '+ tool_name)


