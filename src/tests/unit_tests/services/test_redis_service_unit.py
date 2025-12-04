from unittest.mock import MagicMock, patch
from services.redis import RedisClient


@patch("services.redis.RedisHook")
def test_redis_client_uses_airflow_connection(mock_hook_cls):
    mock_hook = MagicMock()
    mock_conn = MagicMock()
    mock_hook.get_conn.return_value = mock_conn
    mock_hook_cls.return_value = mock_hook

    client_factory = RedisClient(redis_conn_id="redis_app")
    conn = client_factory.create_client()

    mock_hook_cls.assert_called_once_with(redis_conn_id="redis_app")
    mock_hook.get_conn.assert_called_once_with()
    assert conn is mock_conn


def test_redis_client_delete_closes_client_if_supported():
    client_factory = RedisClient()
    mock_client = MagicMock()

    client_factory.delete_client(mock_client)

    mock_client.close.assert_called_once_with()
