from services.redis import RedisClient


def test_redis_airflow_connection_integration() -> None:
    """Ensure we can connect to the Airflow Redis instance using the service client."""
    client_factory = RedisClient(redis_conn_id="redis_airflow")
    client = client_factory.create_client()
    try:
        assert client.ping() is True
    finally:
        client_factory.delete_client(client)


def test_redis_app_connection_integration() -> None:
    """Ensure we can connect to the app Redis instance using the service client."""
    client_factory = RedisClient(redis_conn_id="redis_app")
    client = client_factory.create_client()
    try:
        assert client.ping() is True
    finally:
        client_factory.delete_client(client)
