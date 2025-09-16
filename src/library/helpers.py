"""
Helper functions for ETL template DAGs.

This module provides utility functions for working with template-based
ETL pipelines in Airflow. It includes configuration management through
Airflow Variables and JSON files, simulation of service delays,
concurrency-safe variable updates, and helpers for batching and logging.

Attributes:
    JSON_FILE_PATH: Path to the JSON configuration file with settings.
"""

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


def get_combinations(start: int = 1, end: int = 6, repeat: int = 3) -> list[list[int]]:
    """Generates all combinations of integers within a range.

    Args:
        start: Inclusive start of the range.
        end: Exclusive end of the range.
        repeat: Number of positions to fill.

    Returns:
        A list of integer lists with all possible combinations.

    Example:
        get_combinations(1, 3, 2) → [[1, 1], [1, 2], [2, 1], [2, 2]]
    """
    combinations = list(product(range(start, end), repeat=repeat))
    return [list(tup) for tup in combinations]


def random_sleep(min_seconds: float, max_seconds: float) -> None:
    """Sleeps for a random duration.

    Args:
        min_seconds: Minimum number of seconds.
        max_seconds: Maximum number of seconds.
    """
    time.sleep(random.uniform(min_seconds, max_seconds))


def open_json_file(file_path: str) -> dict:
    """Opens a JSON file.

    Args:
        file_path: Path to the JSON file.

    Returns:
        The file contents as a dictionary.
    """
    with open(file_path, "r") as f:
        return json.load(f)


def get_n_batches() -> int:
    """Gets the number of batches from the JSON configuration.

    Returns:
        The number of batches defined in the config file.
    """
    settings = open_json_file(JSON_FILE_PATH)
    return settings["batches"]


def get_n_dags() -> int:
    """Gets the number of DAGs from the JSON configuration.

    Returns:
        The number of DAGs defined in the config file.
    """
    settings = open_json_file(JSON_FILE_PATH)
    return settings["n_dags"]


def get_etl_variable() -> dict:
    """Gets the ETL configuration variables from the JSON configuration.

    Returns:
        The ETL configuration section of the config file.
    """
    settings = open_json_file(JSON_FILE_PATH)
    return settings["config_variable"]


def simulate_service_time(service_type: str) -> None:
    """Simulates a service call with randomized duration.

    Args:
        service_type: Either "external_services" or "internal_services".
    """
    settings = open_json_file(JSON_FILE_PATH)["execution_simulation"]
    min_seconds = settings[service_type]["min_seconds"]
    max_seconds = settings[service_type]["max_seconds"]
    random_sleep(min_seconds, max_seconds)


def get_variable(var_name: str) -> Dict[str, Any]:
    """Reads an Airflow Variable safely.

    Args:
        var_name: The name of the Airflow Variable.

    Returns:
        The parsed JSON value of the variable, or {} if missing or invalid.
    """
    try:
        return Variable.get(var_name, default_var={}, deserialize_json=True)
    except Exception as e:
        logger.warning("get_variable(%s) failed: %s", var_name, e)
        return {}


def _set_variable_with_retry(var_name: str, data: Dict[str, Any], retries: int = 8) -> None:
    """Concurrency-safe setter for Airflow Variables.

    Retries on IntegrityError to handle parallel insert/update races.

    Args:
        var_name: The name of the Airflow Variable.
        data: The value to set, serialized as JSON.
        retries: Number of retry attempts.

    Raises:
        Exception: If all retries fail.
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
            time.sleep(0.1 + random.random() * 0.2)
        except Exception as e:
            last_err = e
            time.sleep(0.05)
    if last_err:
        raise last_err


def create_config_variable(var_name: str, data: Dict[str, Any]) -> None:
    """Creates a variable if missing; safe under concurrency.

    Args:
        var_name: The name of the variable.
        data: The JSON-serializable data to store.
    """
    current = get_variable(var_name)
    if current:
        return
    _set_variable_with_retry(var_name, data)


def update_config_variable(var_name: str, data: Dict[str, Any]) -> None:
    """Creates or updates a variable with retry.

    Args:
        var_name: The name of the variable.
        data: The JSON-serializable data to store.
    """
    _set_variable_with_retry(var_name, data)


def set_variable(var_name: str, data: Dict[str, Any]) -> None:
    """Always sets a variable safely.

    Args:
        var_name: The name of the variable.
        data: The JSON-serializable data to store.
    """
    _set_variable_with_retry(var_name, data)


def start_delta_seconds() -> float:
    """Gets the current time in seconds.

    Returns:
        The current time as a float.
    """
    return time.time()


def stop_delta_seconds(start: float) -> float:
    """Computes elapsed time since a start timestamp.

    Args:
        start: Start time in seconds.

    Returns:
        Elapsed time in seconds, rounded to milliseconds.
    """
    return round(time.time() - start, 3)


def batches(task_type: str):
    """Yields batch numbers for a task type.

    Args:
        task_type: One of "extractors", "transformers", or "loaders".

    Yields:
        Tuples of (batch_number, total_batches) for the given task type.
    """
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
    """Parses ETL distribution counts from DAG run configuration.

    Returns:
        A tuple (n_extractors, n_transformers, n_loaders).
    """
    ctx = get_current_context()
    dag_run = ctx["dag_run"]
    var = dag_run.conf.get("var")
    config_value = Variable.get(var)
    n_extractors, n_transformers, n_loaders = map(int, config_value.split("-"))
    return n_extractors, n_transformers, n_loaders


def get_log(n: int, n_task_batches: int, time_spent: float):
    """Logs task completion progress.

    Uses DAG run conf/Variable to determine run_type.

    Args:
        n: Current batch number.
        n_task_batches: Total number of batches.
        time_spent: Time spent on the task in seconds.
    """
    n_extractors, n_transformers, n_loaders = get_etl_distribution()
    run_type = f"{n_extractors}-{n_transformers}-{n_loaders}"
    completion = round((n / max(n_task_batches, 1)) * 100, 2)
    logger.info("%s %s %s %s", run_type, n, completion, time_spent)
