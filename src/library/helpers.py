import random
import time
import json

JSON_FILE_PATH = "/opt/airflow/dags/app_template/config/app_settings.json"

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

def simulate_service_time(service_type: str) -> None:
    """
    Simulate a service call. Service type can be 'external_services' 
    or 'internal_services'.
    """
    settings = open_json_file(JSON_FILE_PATH)['execution_simulation']
    min_seconds = settings[service_type]['min_seconds']
    max_seconds = settings[service_type]['max_seconds']
    random_sleep(min_seconds, max_seconds)
    