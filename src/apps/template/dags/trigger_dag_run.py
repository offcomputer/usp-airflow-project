from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="trigger_app_template",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["development", "template"],
) as dag:

    for i in range(5):
        BashOperator(
            task_id=f"run_template_{i+1}",
            # Each parallel task gets a unique slot id
            bash_command=f"python /opt/airflow/dags/library/run_template.py --slot {i}",
        )
