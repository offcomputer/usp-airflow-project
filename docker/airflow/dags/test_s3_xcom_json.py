from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
}


def push_json(**context):
    payload = {"foo": "bar", "answer": 42, "nested": {"hello": "world"}}
    # Push JSON-serializable payload to XCom (Airflow will store it as JSON if backend supports it)
    context["ti"].xcom_push(key="sample_json", value=payload)


def pull_json(**context):
    data = context["ti"].xcom_pull(key="sample_json", task_ids="push_json")
    print(f"Pulled JSON from XCom: {data}")


with DAG(
    dag_id="test_s3_xcom_json",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["test", "xcom", "s3", "json"],
) as dag:
    push = PythonOperator(
        task_id="push_json",
        python_callable=push_json,
    )

    pull = PythonOperator(
        task_id="pull_json",
        python_callable=pull_json,
    )

    push >> pull
