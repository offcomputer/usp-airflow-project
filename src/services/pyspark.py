from typing import Any, Dict, Optional
from airflow.hooks.base import BaseHook
from interfaces.clients import ServiceClientInterface
from pyspark.sql import SparkSession


class PySparkClient(ServiceClientInterface):
    """Factory for PySpark sessions using the shared client interface."""

    def __init__(
        self,
        master: Optional[str] = None,
        app_name: str = "app",
        configs: Optional[Dict[str, str]] = None,
        spark_conn_id: str = "spark_default",
    ) -> None:
        self._master = master
        self._app_name = app_name
        self._configs = configs or {}
        self._spark_conn_id = spark_conn_id

    def create_client(self, **kwargs: Any) -> SparkSession:
        """
        Create and return a SparkSession configured with the provided
        master/app_name/configs (kwargs override constructor defaults).
        """
        conn_id = kwargs.get("spark_conn_id", self._spark_conn_id)
        master_override = kwargs.get("master") or self._master

        conn = None
        if not master_override and conn_id:
            conn = BaseHook.get_connection(conn_id)

        master = master_override or (conn.get_uri() if conn else None)
        app_name = kwargs.get("app_name", self._app_name)
        extra_configs = {**self._configs}
        if conn:
            extra_configs.update(conn.extra_dejson)
        extra_configs.update(kwargs.get("configs", {}))

        builder = SparkSession.builder
        if master:
            builder = builder.master(master)
        if app_name:
            builder = builder.appName(app_name)
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    def delete_client(self, client: Any) -> None:
        """Stop the given SparkSession."""
        stop_method = getattr(client, "stop", None)
        if callable(stop_method):
            stop_method()
