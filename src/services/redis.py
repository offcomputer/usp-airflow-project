from typing import Any
from airflow.providers.redis.hooks.redis import RedisHook
from interfaces.clients import ServiceClientInterface


class RedisClient(ServiceClientInterface):
    """Factory for Redis clients using an Airflow Redis connection."""

    def __init__(self, redis_conn_id: str = "redis_airflow") -> None:
        self._redis_conn_id = redis_conn_id

    def create_client(self, **kwargs: Any):
        """Create and return a Redis client obtained via RedisHook and
        the configured Airflow connection."""
        hook = RedisHook(
            redis_conn_id=kwargs.get("redis_conn_id", self._redis_conn_id)
        )
        return hook.get_conn()

    def delete_client(self, client: Any) -> None:
        """Close the given Redis client if it supports close()."""
        close_method = getattr(client, "close", None)
        if callable(close_method):
            close_method()
