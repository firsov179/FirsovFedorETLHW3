from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from postgres_utils import load_to_postgres

def load_hottest(**context):
    path = '/opt/airflow/data/hottest_days.csv'

    if os.path.exists(path):
        df = pd.read_csv(path)
        print(f"Загружено {len(df)} записей самых горячих дней")
        print(f"Данные:\n{df.to_string()}")

        rows = load_to_postgres('hottest_days', df)

        return f"Успешно загружено {rows} самых горячих дней"
    else:
        raise FileNotFoundError(f"Файл {path} не найден")

def load_coldest(**context):
    path = '/opt/airflow/data/coldest_days.csv'

    if os.path.exists(path):
        df = pd.read_csv(path)
        rows = load_to_postgres('coldest_days', df)

        return f"Успешно загружено {rows} самых холодных дней"
    else:
        raise FileNotFoundError(f"Файл {path} не найден")

with DAG(
    dag_id='load_historical_data',
    schedule=None,
    catchup=False,
) as dag:

    hot = PythonOperator(
        task_id='load_hottest',
        python_callable=load_hottest,
    )

    cold = PythonOperator(
        task_id='load_coldest',
        python_callable=load_coldest,
    )

    [hot, cold]
