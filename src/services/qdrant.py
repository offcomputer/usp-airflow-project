from typing import Any

from airflow.hooks.base import BaseHook
from interfaces.clients import ServiceClientInterface
from qdrant_client import QdrantClient


class QdrantDBClient(ServiceClientInterface):
    """Factory for Qdrant clients using an Airflow HTTP connection."""

    def __init__(self, qdrant_conn_id: str = "qdrant_db") -> None:
        self._qdrant_conn_id = qdrant_conn_id

    def create_client(self, **kwargs: Any) -> QdrantClient:
        """
        Create and return a QdrantClient configured from an Airflow
        connection.

        The Airflow connection ``qdrant_conn_id`` is expected to be of
        type HTTP, with host/port (and optional schema, login and
        password) defining the Qdrant endpoint URL.
        """
        conn_id = kwargs.get("qdrant_conn_id", self._qdrant_conn_id)
        conn = BaseHook.get_connection(conn_id)
        url = conn.get_uri()
        return QdrantClient(url=url)

    def delete_client(self, client: Any) -> None:
        """
        Close the given Qdrant client if it supports close().
        """
        client.close()
