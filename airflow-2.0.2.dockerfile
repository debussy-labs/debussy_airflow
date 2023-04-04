FROM apache/airflow:2.0.2

COPY requirements.airflow-2.0.2.txt requirements.txt

RUN pip install -r requirements.txt