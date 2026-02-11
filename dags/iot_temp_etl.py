from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

def etl_iot_temp():
    df = pd.read_csv('/opt/airflow/data/IOT-temp.csv', names=['id', 'room_id', 'noted_date', 'temp', 'out_in'])

    df['temp'] = pd.to_numeric(df['temp'], errors='coerce')
    df = df.dropna(subset=['temp'])

    df = df[df['out_in'].str.lower() == 'in']
    df['noted_date'] = pd.to_datetime(df['noted_date'], dayfirst=True)
    df['date_only'] = df['noted_date'].dt.date
    df_filtered = df[(df['temp'] >= df['temp'].quantile(0.05)) & (df['temp'] <= df['temp'].quantile(0.95))]
    daily_temps = df_filtered.groupby('date_only')['temp'].mean().reset_index()
    hottest_days = daily_temps.nlargest(5, 'temp')
    coldest_days = daily_temps.nsmallest(5, 'temp')
    hottest_days.to_csv('/opt/airflow/data/hottest_days.csv', index=False)
    coldest_days.to_csv('/opt/airflow/data/coldest_days.csv', index=False)

with DAG(
    dag_id='iot_temp_etl',
    schedule=None,
    catchup=False,
) as dag:

    process_nutrition_task = PythonOperator(
        task_id="etl_iot_temp",
        python_callable=etl_iot_temp,
    )
