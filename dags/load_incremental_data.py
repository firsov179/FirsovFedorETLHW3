from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from postgres_utils import load_to_postgres

def process_data(**context):
    path = '/opt/airflow/data/IOT-temp.csv'
    days = 7

    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл {path} не найден")

    df = pd.read_csv(path, names=['id', 'room_id', 'noted_date', 'temp', 'out_in'])

    df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
    df = df.dropna(subset=['temp'])
    df = df[df['out_in'].str.lower() == 'in']
    df['noted_date'] = pd.to_datetime(df['noted_date'], dayfirst=True)
    df['date_only'] = df['noted_date'].dt.date

    cutoff = (datetime.now() - timedelta(days=days)).date()
    df_new = df[df['date_only'] >= cutoff]

    if len(df_new) == 0:
        return {"hot_cnt": 0, "cold_cnt": 0}

    df_filt = df_new[
        (df_new['temp'] >= df_new['temp'].quantile(0.05)) &
        (df_new['temp'] <= df_new['temp'].quantile(0.95))
    ]

    temps = df_filt.groupby('date_only')['temp'].mean().reset_index()

    if len(temps) == 0:
        return {"hot_cnt": 0, "cold_cnt": 0}

    hot = temps.nlargest(5, 'temp')
    cold = temps.nsmallest(5, 'temp')

    hot_cnt = load_to_postgres('hottest_days', hot)
    cold_cnt = load_to_postgres('coldest_days', cold)

    return {
        "hot_cnt": len(hot),
        "cold_cnt": len(cold),
        "hot": hot.to_dict('records'),
        "cold": cold.to_dict('records'),
        "hot_loaded": hot_cnt,
        "cold_loaded": cold_cnt
    }

def load_hottest(**context):
    ti = context['ti']
    result = ti.xcom_pull(task_ids='process_data')

    if result and result.get('hot_loaded', 0) > 0:
        return f"Успешно загружено {result.get('hot_loaded', 0)} самых горячих дней"
    else:
        return "Нет данных"

def load_coldest(**context):
    ti = context['ti']
    result = ti.xcom_pull(task_ids='process_data')

    if result and result.get('cold_loaded', 0) > 0:
        return f"Успешно загружено {result.get('cold_loaded', 0)} самых холодных дней"
    else:
        return "Нет данных"

with DAG(
    dag_id='load_incremental_data',
    schedule=timedelta(days=1),
    catchup=False,
) as dag:

    process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    hot = PythonOperator(
        task_id='load_hottest',
        python_callable=load_hottest,
    )

    cold = PythonOperator(
        task_id='load_coldest',
        python_callable=load_coldest,
    )

    process >> [hot, cold]
