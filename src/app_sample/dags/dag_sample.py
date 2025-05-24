from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello():
    print("Hello from Airflow!")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hello_world_dag",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:
    
    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello,
    )
