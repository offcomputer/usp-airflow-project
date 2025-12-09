"""Run recsys notebooks inside the running RAPIDS container via Papermill."""

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

RAPIDS_SVC = "rapids-notebook"  # container name from docker-rapids-compose.yml
# Notebooks live under src/notebooks/recsys; src is mounted to /home/rapids/notebooks
BASE_NB = "/home/rapids/notebooks/notebooks/recsys"
OUT_DIR = "/home/rapids/shared_data/recsys_runs"

default_args = {"owner": "data-eng", "retries": 0}


def pm_cmd(nb_filename: str, params: dict | None = None) -> str:
    """Compose a docker exec command that runs papermill inside the RAPIDS container."""
    params = params or {}
    param_args = " ".join(f"-p {k} '{v}'" for k, v in params.items())
    return (
        f"docker exec {RAPIDS_SVC} bash -lc "
        f"\"mkdir -p {OUT_DIR} && "
        f"papermill {BASE_NB}/{nb_filename} {OUT_DIR}/{nb_filename.replace('.ipynb', '')}_out.ipynb {param_args}\""
    )


with DAG(
    dag_id="recsys_notebooks_on_running_rapids",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["recsys", "rapids", "papermill"],
) as dag:
    n01 = BashOperator(
        task_id="run_01_files_evaluation",
        bash_command=pm_cmd("01_files_evaluation.ipynb", {"run_ts": "{{ ts }}"}),
    )

    n02 = BashOperator(
        task_id="run_02_eda",
        bash_command=pm_cmd("02_eda.ipynb", {"run_ts": "{{ ts }}"}),
    )

    n03 = BashOperator(
        task_id="run_03_feature_engineering",
        bash_command=pm_cmd("03_feature_engineering.ipynb"),
    )

    n01 >> n02 >> n03
