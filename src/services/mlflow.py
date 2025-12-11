from typing import Any
import mlflow
from airflow.hooks.base import BaseHook
from interfaces.clients import ServiceClientInterface


class MlflowClient(ServiceClientInterface):
    """Factory for MLflow clients using an Airflow HTTP connection."""

    def __init__(self, mlflow_conn_id: str = "mlflow_tracking") -> None:
        self._mlflow_conn_id = mlflow_conn_id

    def create_client(self, **kwargs: Any) -> mlflow.MlflowClient:
        """
        Create and return an MlflowClient configured from an Airflow
        connection.

        The Airflow connection ``mlflow_conn_id`` is expected to be of
        type HTTP, with host/port (and optional schema, login and
        password) defining the tracking URI.
        """
        conn_id = kwargs.get("mlflow_conn_id", self._mlflow_conn_id)
        conn = BaseHook.get_connection(conn_id)
        tracking_uri = conn.get_uri()
        mlflow.set_tracking_uri(tracking_uri)
        return mlflow.MlflowClient(tracking_uri=tracking_uri)

    def delete_client(self, client: Any) -> None:
        """
        MlflowClient does not require explicit disposal, but this method
        exists to satisfy the interface and for symmetry with other
        service clients.
        """
        return None
