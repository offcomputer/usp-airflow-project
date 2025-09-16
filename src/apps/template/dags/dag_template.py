"""
Dynamic ETL DAG template.

This module defines one or more ETL DAGs in Apache Airflow. Each DAG
consists of three stages: extract, transform, and load. The tasks are
organized into TaskGroups, and the number of tasks in each stage is
configurable via Airflow Variables.

Each DAG run must provide a configuration in dag_run.conf that includes
a "var" key. That key must point to the name of an Airflow Variable
containing a string of the form "<extractors>-<transformers>-<loaders>".

Attributes:
    MAX_EXTRACTORS: Maximum number of extractor tasks.
    MAX_TRANSFORMERS: Maximum number of transformer tasks.
    MAX_LOADERS: Maximum number of loader tasks.

Typical usage example:
    Trigger a DAG with a specific configuration:

        airflow dags trigger app_template_1 \
            --conf '{"var": "my_config_var"}'

    If the Airflow Variable "my_config_var" is set to "3-2-1", the DAG
    will run 3 extractors, 2 transformers, and 1 loader.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime
import library.helpers as helpers
import apps.template.tasks.etl_tasks as tasks


MAX_EXTRACTORS = 5
MAX_TRANSFORMERS = 5
MAX_LOADERS = 5

n_dags = helpers.get_n_dags()
for d in range(1, n_dags + 1):
    with DAG(
        dag_id=f"app_template_{d}",
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,
        catchup=False,
        description="ETL DAG with up to 5x5x5 tasks, counts controlled by Variables",
    ) as dag:

        def should_run(kind: str, index: int, **context):
            """Determines whether a task of a given type should run.

            The decision is based on a Variable configured in Airflow,
            provided via dag_run.conf["var"]. The variable must contain
            a dash-separated string with counts for extractors,
            transformers, and loaders.

            Args:
                kind: The task type ("extractors", "transformers", or "loaders").
                index: The zero-based index of the task in its group.
                **context: Airflow task context, including dag_run.conf.

            Returns:
                True if the task index is within the configured limit,
                False otherwise.

            Raises:
                ValueError: If the run configuration does not contain a "var" key.
            """
            run_conf = context["dag_run"].conf or {}
            var_name = run_conf.get("var")
            if not var_name:
                raise ValueError("conf must include a 'var' key")

            value = Variable.get(var_name) 
            parts = [int(x) for x in value.split("-")]
            cfg = {"extractors": parts[0], "transformers": parts[1], "loaders": parts[2]}
            return index < cfg[kind] 

        def extractor_callable(index, **context):
            """Runs or skips an extractor task.

            Args:
                index: The zero-based index of the extractor.
                **context: Airflow task context.

            Returns:
                The result of tasks.extract() if executed, None otherwise.
            """
            if should_run("extractors", index, **context):
                return tasks.extract()
            else:
                print(f"Skipping extractor {index+1}")

        def transformer_callable(index, **context):
            """Runs or skips a transformer task.

            Args:
                index: The zero-based index of the transformer.
                **context: Airflow task context.

            Returns:
                The result of tasks.transform() if executed, None otherwise.
            """
            if should_run("transformers", index, **context):
                return tasks.transform()
            else:
                print(f"Skipping transformer {index+1}")

        def loader_callable(index, **context):
            """Runs or skips a loader task.

            Args:
                index: The zero-based index of the loader.
                **context: Airflow task context.

            Returns:
                The result of tasks.load() if executed, None otherwise.
            """
            if should_run("loaders", index, **context):
                return tasks.load()
            else:
                print(f"Skipping loader {index+1}")

        with TaskGroup("extractors") as extract_group:
            for i in range(MAX_EXTRACTORS):
                PythonOperator(
                    task_id=f"extract_{i+1}",
                    python_callable=extractor_callable,
                    op_kwargs={"index": i},
                )

        with TaskGroup("transformers") as transform_group:
            for i in range(MAX_TRANSFORMERS):
                PythonOperator(
                    task_id=f"transform_{i+1}",
                    python_callable=transformer_callable,
                    op_kwargs={"index": i},
                )

        with TaskGroup("loaders") as load_group:
            for i in range(MAX_LOADERS):
                PythonOperator(
                    task_id=f"load_{i+1}",
                    python_callable=loader_callable,
                    op_kwargs={"index": i},
                )
