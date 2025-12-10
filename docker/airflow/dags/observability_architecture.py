"""
Documentation-only DAG that visualizes the observability topology.

It uses EmptyOperator tasks as nodes so the Graph view shows how metrics
and logs flow through exporters, Prometheus, Loki, Grafana, and Flower.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="observability_architecture",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=r"""
### Observability topology
Metrics: services → exporters/StatsD → Prometheus → Grafana  
Logs: containers/Airflow task logs → Promtail → Loki → Grafana  
Celery: workers/broker → Flower UI; Celery StatsD → Prometheus
""",
) as dag:
    # Sources
    airflow_web = EmptyOperator(task_id="airflow_web")
    airflow_scheduler = EmptyOperator(task_id="airflow_scheduler")
    airflow_workers = EmptyOperator(task_id="airflow_workers_celery")
    airflow_triggerer = EmptyOperator(task_id="airflow_triggerer")
    flower = EmptyOperator(task_id="flower_ui")
    postgres = EmptyOperator(task_id="postgres")
    redis_airflow = EmptyOperator(task_id="redis_airflow_broker")
    redis_app = EmptyOperator(task_id="redis_app")
    minio = EmptyOperator(task_id="minio")
    qdrant = EmptyOperator(task_id="qdrant")
    grafana = EmptyOperator(task_id="grafana")
    prometheus_self = EmptyOperator(task_id="prometheus_self")

    # Metrics/exporters
    statsd_exporter = EmptyOperator(task_id="statsd_exporter_from_celery")
    postgres_exporter = EmptyOperator(task_id="postgres_exporter")
    redis_airflow_exporter = EmptyOperator(task_id="redis_airflow_exporter")
    redis_app_exporter = EmptyOperator(task_id="redis_app_exporter")
    minio_cluster_exporter = EmptyOperator(task_id="minio_cluster_metrics")
    minio_node_exporter = EmptyOperator(task_id="minio_node_metrics")
    qdrant_exporter = EmptyOperator(task_id="qdrant_metrics")

    prometheus = EmptyOperator(task_id="prometheus_scrape")
    grafana_metrics = EmptyOperator(task_id="grafana_metrics_dashboards")

    # Logs
    promtail = EmptyOperator(task_id="promtail_docker_sd_airflow_logs")
    loki = EmptyOperator(task_id="loki_log_store")
    grafana_logs = EmptyOperator(task_id="grafana_logs_explorer")

    # Metrics flow edges
    airflow_workers >> statsd_exporter >> prometheus
    airflow_scheduler >> statsd_exporter
    airflow_triggerer >> statsd_exporter
    airflow_web >> statsd_exporter
    postgres >> postgres_exporter >> prometheus
    redis_airflow >> redis_airflow_exporter >> prometheus
    redis_app >> redis_app_exporter >> prometheus
    minio >> [minio_cluster_exporter, minio_node_exporter] >> prometheus
    qdrant >> qdrant_exporter >> prometheus
    grafana >> prometheus
    prometheus_self >> prometheus
    prometheus >> grafana_metrics

    # Logs flow edges
    [
        airflow_web,
        airflow_scheduler,
        airflow_workers,
        airflow_triggerer,
        flower,
        postgres,
        redis_airflow,
        redis_app,
        minio,
        qdrant,
        grafana,
        prometheus_self,
    ] >> promtail >> loki >> grafana_logs

    # Celery UI
    [redis_airflow, airflow_workers] >> flower
