"""
Run all template DAGs sequentially.

This module defines a single Airflow DAG that triggers the execution of
the run_template helper script for each configured template DAG. The
tasks are created dynamically based on the number of DAGs specified by
helpers.get_n_dags() and are chained sequentially, so each template is
executed only after the previous one has finished.

Typical usage example:
    airflow dags trigger run_all_templates
"""

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
