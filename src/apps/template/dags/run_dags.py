from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import library.helpers as helpers


with DAG(
    dag_id="run_all_templates",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Run the run_template helper for DAGs 1..10 sequentially",
) as dag:
    n_dags = helpers.get_n_dags()
    prev_task = None
    for i in range(1, n_dags + 1):
        current_task = BashOperator(
            task_id=f"run_template_{i}",
            bash_command=f"python /opt/airflow/dags/library/run_template.py --dag {i}",
        )

        if prev_task:
            prev_task >> current_task  # set sequential dependency

        prev_task = current_task
