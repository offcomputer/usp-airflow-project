from unittest.mock import MagicMock, patch
from services.postgres import PostgresDBClient


@patch("services.postgres.PostgresHook")
def test_postgres_client_uses_airflow_connection(mock_hook_cls):
    mock_hook = MagicMock()
    mock_conn = MagicMock()
    mock_hook.get_conn.return_value = mock_conn
    mock_hook_cls.return_value = mock_hook

    client_factory = PostgresDBClient(postgres_conn_id="postgres_mlflow")
    conn = client_factory.create_client()

    mock_hook_cls.assert_called_once_with(postgres_conn_id="postgres_mlflow")
    mock_hook.get_conn.assert_called_once_with()
    assert conn is mock_conn


def test_postgres_client_delete_closes_connection():
    client_factory = PostgresDBClient()
    mock_conn = MagicMock()
    mock_conn.closed = False

    client_factory.delete_client(mock_conn)

    mock_conn.close.assert_called_once_with()
