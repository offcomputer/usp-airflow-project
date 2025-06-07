import time
import uuid
import subprocess
from airflow.models import DagRun, DagModel
from airflow.utils.state import State
from airflow.utils.session import create_session
import library.helpers as helpers


def unpause_dag(dag_id):
    with create_session() as session:
        (session
         .query(DagModel)
         .filter(DagModel.dag_id == dag_id)
         .update({DagModel.is_paused: False}))
        session.commit()


def run_dag_with_config(dag_id, run_conf):
    helpers.update_config_variable("app_template_config", run_conf)
    unpause_dag(dag_id)
    run_id = f"manual__{uuid.uuid4()}"
    subprocess.run(["airflow", "dags", "trigger", dag_id, "--run-id", run_id], 
                   check=True)
    print(
        f"Triggered {dag_id} with run_id {run_id}. Waiting for completion...")
    while True:
        with create_session() as session:
            dag_run = (session.query(DagRun)
                       .filter_by(dag_id=dag_id, run_id=run_id)
                       .first())
            if dag_run and dag_run.state in [State.SUCCESS, State.FAILED]:
                print(
                    f"DAG run {run_id} completed with state: {dag_run.state}")
                break
        time.sleep(10)


def main():
    dag_id = "app_template"
    base_config = helpers.get_variable("app_template_config")
    task_distribution = base_config.get("task_distribution", [])

    for x, y, z in task_distribution:
        print(f"\n--- DAG: extractors={x}, transformers={y}, loaders={z} ---")
        updated_config = {
            **base_config,
            "extractors": x,
            "transformers": y,
            "loaders": z,
        }
        run_dag_with_config(dag_id, updated_config)


if __name__ == "__main__":
    main()
