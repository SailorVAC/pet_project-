import sys
sys.path.append("/opt/airflow/src")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract_likes import main as extract_main
from load_likes import load_main as load_main


with DAG(
    dag_id="yandex_likes_etl",
    start_date=datetime(2025, 8, 14),
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="extract_likes",
        python_callable=extract_main
    )

    t2 = PythonOperator(
        task_id="load_likes",
        python_callable=load_main
    )

    t1 >> t2
