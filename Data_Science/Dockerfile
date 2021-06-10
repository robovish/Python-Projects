FROM python:3.7
EXPOSE 8501
WORKDIR /app
COPY . /app
RUN pip install -r ./resources/requirements.txt
CMD streamlit run ./src/app.py