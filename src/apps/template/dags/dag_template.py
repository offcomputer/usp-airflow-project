from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import library.helpers as helpers
import apps.template.tasks.etl_tasks as tasks

# The balancing between operators can be set 
# at "http://localhost:8080/variable/list/"
helpers.create_config_variable(
    var_name="app_template_config",
    data={
        "extractors": 1,
        "transformers": 1,
        "loaders": 1
    }
)
# Load the configuration variable from Airflow Variables
CONFIG = helpers.get_variable("app_template_config")

with DAG(
    dag_id="app_template",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Parallel extract, transform, and load task groups",
) as dag:

    with TaskGroup("extractors") as extract_group:
        for i in range(CONFIG.get("extractors")):
            PythonOperator(
                task_id=f"extract_{i+1}",
                python_callable=tasks.extract,
            )

    with TaskGroup("transformers") as transform_group:
        for i in range(CONFIG.get("transformers")):
            PythonOperator(
                task_id=f"transform_{i+1}",
                python_callable=tasks.transform,
            )

    with TaskGroup("loaders") as load_group:
        for i in range(CONFIG.get("loaders")):
            PythonOperator(
                task_id=f"load_{i+1}",
                python_callable=tasks.load,
            )
