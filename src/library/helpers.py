import random
import time
import json
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
logger = LoggingMixin().log

JSON_FILE_PATH = "/opt/airflow/dags/apps/template/config/app_settings.json"

def random_sleep(min_seconds: float, max_seconds: float) -> None:
    """
    Sleep for a random amount of time between min_seconds and 
    max_seconds (inclusive).
    """
    duration = random.uniform(min_seconds, max_seconds)
    time.sleep(duration)

def open_json_file(file_path: str) -> dict:
    """
    Open a JSON file and return its contents as a dictionary.
    """
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def get_n_batches() -> int:
    """
    Get the number of batches to process from the JSON 
    configuration file.
    """
    settings = open_json_file(JSON_FILE_PATH)
    return settings['batches']

def simulate_service_time(service_type: str) -> None:
    """
    Simulate a service call. Service type can be 'external_services' 
    or 'internal_services'.
    """
    settings = open_json_file(JSON_FILE_PATH)['execution_simulation']
    min_seconds = settings[service_type]['min_seconds']
    max_seconds = settings[service_type]['max_seconds']
    random_sleep(min_seconds, max_seconds)

def get_variable(var_name: str) -> dict[str, int]:
    """
    Reads an Airflow Variable, parses it as JSON, and returns it as a 
    dictionary.
    """
    try:
        json_data = Variable.get(var_name)
        parsed_data = json.loads(json_data)
        return parsed_data
    except:
        return {}

def set_variable(var_name: str, data: dict[str, int]) -> None:
    """
    Creates or updates an Airflow Variable from a dictionary.
    """
    json_data = json.dumps(data)
    Variable.set(var_name, json_data)

def create_config_variable(var_name: str, data: dict[str, int]) -> None:
    """
    Sets an Airflow Variable to a string value.
    """
    parsed_data = get_variable(var_name)
    if parsed_data:
        return
    set_variable(var_name, data)

def start_delta_seconds() -> float:
    """
    Returns the current time in seconds as a float.
    """
    return time.time()

def stop_delta_seconds(start: float) -> float:
    """
    Returns the elapsed time in seconds as a float (rounded to milliseconds).
    """
    end = time.time()
    return round(end - start, 3)

def batches(task_type: str):
    n_batches = get_n_batches()
    config = get_variable("app_template_config")
    n_tasks = config.get(task_type, 1)
    n_task_batches = n_batches // n_tasks
    for n in range(n_task_batches):
        n += 1
        yield n, n_task_batches

def get_log(n: int, n_task_batches: int, time_spent: float):
    """
    Log the counter and the time spent on the task.
    """
    logger.info(f'{n}:{n_task_batches}:{time_spent}')

