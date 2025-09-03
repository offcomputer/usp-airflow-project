from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import library.helpers as helpers
import apps.template.tasks.etl_tasks as tasks


n_dags = helpers.get_n_dags()
for d in range(1, n_dags + 1):
    var_name = f"app_template_config_{d}"
    helpers.create_config_variable(
        var_name=var_name,
        data=helpers.get_etl_variable()
    )
    defaults = helpers.get_variable(var_name)

    with DAG(
        dag_id=f"app_template_{d}",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        description="Parallel extract, transform, and load task groups",
    ) as dag:

        # Pull config from conf (runtime) or Variable (default)
        def get_config(**context):
            conf = context["dag_run"].conf if context.get("dag_run") else {}
            return {
                "extractors": int(conf.get("extractors", defaults.get("extractors", 1))),
                "transformers": int(conf.get("transformers", defaults.get("transformers", 1))),
                "loaders": int(conf.get("loaders", defaults.get("loaders", 1))),
            }

        cfg = get_config()

        with TaskGroup("extractors") as extract_group:
            for i in range(cfg["extractors"]):
                PythonOperator(
                    task_id=f"extract_{i+1}",
                    python_callable=tasks.extract,
                )

        with TaskGroup("transformers") as transform_group:
            for i in range(cfg["transformers"]):
                PythonOperator(
                    task_id=f"transform_{i+1}",
                    python_callable=tasks.transform,
                )

        with TaskGroup("loaders") as load_group:
            for i in range(cfg["loaders"]):
                PythonOperator(
                    task_id=f"load_{i+1}",
                    python_callable=tasks.load,
                )

        extract_group >> transform_group >> load_group
