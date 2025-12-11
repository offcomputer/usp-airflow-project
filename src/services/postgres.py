from typing import Any
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import connection as PGConnection
from interfaces.clients import ServiceClientInterface


class PostgresDBClient(ServiceClientInterface):
    """Factory for PostgreSQL connections using an Airflow Postgres
    connection."""

    def __init__(self, postgres_conn_id: str = "postgres_airflow") -> None:
        self._postgres_conn_id = postgres_conn_id

    def create_client(self, **kwargs: Any) -> PGConnection:
        """Create and return a psycopg2 connection obtained via
        PostgresHook and the configured Airflow connection."""
        hook = PostgresHook(
            postgres_conn_id=kwargs.get(
                "postgres_conn_id", self._postgres_conn_id
            )
        )
        return hook.get_conn()

    def delete_client(self, client: Any) -> None:
        """Close the given psycopg2 connection if it is open."""
        try:
            if client and not client.closed:
                client.close()
        except AttributeError:
            # Not a psycopg2 connection; ignore.
            pass
