import json
import random
import time
from typing import Any, Dict
from itertools import product
from airflow.models import Variable
from airflow.utils.session import create_session
from sqlalchemy.exc import IntegrityError
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import get_current_context

logger = LoggingMixin().log

JSON_FILE_PATH = "/opt/airflow/dags/apps/template/config/app_settings.json"


def get_combinations(
        start: int = 1, end: int = 6, repeat: int = 3) -> list[list[int]]:
    """
    Generate all combinations of integers from start to end (exclusive) 
    for 3 positions.
    For example, if start=1 and end=6, it generates combinations like:
    [[1, 1, 1], [1, 1, 2], ..., [5, 5, 5]].
    """
    combinations = list(product(range(start, end), repeat=repeat))
    combinations = [list(tup) for tup in combinations]
    return combinations

def random_sleep(min_seconds: float, max_seconds: float) -> None:
    """
    Sleep for a random amount of time between min_seconds and 
    max_seconds (inclusive).
    """
    time.sleep(random.uniform(min_seconds, max_seconds))

def open_json_file(file_path: str) -> dict:
    """
    Open a JSON file and return its contents as a dictionary.
    """
    with open(file_path, "r") as f:
        return json.load(f)

def get_n_batches() -> int:
    """
    Get the number of batches to process from the JSON 
    configuration file.
    """
    settings = open_json_file(JSON_FILE_PATH)
    return settings["batches"]

def get_n_dags() -> int:
    """
    Get the number of DAGs to process from the JSON configuration file.
    """
    settings = open_json_file(JSON_FILE_PATH)
    return settings["n_dags"]

def get_etl_variable() -> dict:
    """
    Get the ETL configuration variables from the JSON configuration file.
    """
    settings = open_json_file(JSON_FILE_PATH)
    return settings["config_variable"]

def simulate_service_time(service_type: str) -> None:
    """
    Simulate a service call. Service type can be 'external_services' 
    or 'internal_services'.
    """
    settings = open_json_file(JSON_FILE_PATH)["execution_simulation"]
    min_seconds = settings[service_type]["min_seconds"]
    max_seconds = settings[service_type]["max_seconds"]
    random_sleep(min_seconds, max_seconds)

def get_variable(var_name: str) -> Dict[str, Any]:
    """Reads an Airflow Variable (JSON) safely; returns {} if missing/invalid."""
    try:
        return Variable.get(var_name, default_var={}, deserialize_json=True)
    except Exception as e:
        logger.warning("get_variable(%s) failed: %s", var_name, e)
        return {}

def _set_variable_with_retry(var_name: str, data: Dict[str, Any], retries: int = 8) -> None:
    """
    Concurrency-safe setter that relies on Variable.set (handles update-or-insert).
    We just retry on IntegrityError races.
    """
    last_err = None
    for _ in range(retries):
        try:
            with create_session() as session:
                Variable.set(var_name, data, serialize_json=True, session=session)
                session.commit()
            return
        except IntegrityError as e:
            last_err = e
            # Another parallel writer inserted the row — jitter, then retry.
            time.sleep(0.1 + random.random() * 0.2)
        except Exception as e:
            last_err = e
            time.sleep(0.05)
    if last_err:
        raise last_err

def create_config_variable(var_name: str, data: Dict[str, Any]) -> None:
    """Create if missing; no-op if exists. Safe under concurrency."""
    current = get_variable(var_name)
    if current:
        return
    _set_variable_with_retry(var_name, data)

def update_config_variable(var_name: str, data: Dict[str, Any]) -> None:
    """Create-or-update with retry; safe under concurrency."""
    _set_variable_with_retry(var_name, data)

def set_variable(var_name: str, data: Dict[str, Any]) -> None:
    """Backwards-compat convenience: always safe set."""
    _set_variable_with_retry(var_name, data)

def start_delta_seconds() -> float:
    """
    Returns the current time in seconds as a float.
    """
    return time.time()

def stop_delta_seconds(start: float) -> float:
    """
    Returns the elapsed time in seconds as a float (rounded to milliseconds).
    """
    return round(time.time() - start, 3)

def batches(task_type: str):
    n_batches = get_n_batches()
    n_extractors, n_transformers, n_loaders = get_etl_distribution()
    config = {
        "extractors": n_extractors,
        "transformers": n_transformers,
        "loaders": n_loaders,
    }
    n_tasks = config.get(task_type, 1)
    n_task_batches = n_batches // n_tasks if n_tasks else 0
    for n in range(n_task_batches):
        yield n + 1, n_task_batches

def get_etl_distribution():
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    var = dag_run.conf.get("var")
    config_value = Variable.get(var)
    n_extractors, n_transformers, n_loaders = map(int, config_value.split("-"))
    return n_extractors, n_transformers, n_loaders

def get_log(n: int, n_task_batches: int, time_spent: float):
    """
    Log the counter and the time spent on the task.
    Uses the current DAG run's conf/Variable to determine run_type.
    """
    n_extractors, n_transformers, n_loaders = get_etl_distribution()
    run_type = f"{n_extractors}-{n_transformers}-{n_loaders}"
    completion = round((n / max(n_task_batches, 1)) * 100, 2)
    logger.info("%s %s %s %s", run_type, n, completion, time_spent)
