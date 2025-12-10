from typing import Any
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.client import BaseClient
from interfaces.clients import ServiceClientInterface


class MinIOObjectClient(ServiceClientInterface):
    """Factory for a MinIO (S3-compatible) client using an Airflow
    S3/MinIO connection."""

    def __init__(
        self,
        aws_conn_id: str = "minio_s3",
    ) -> None:
        self._aws_conn_id = aws_conn_id

    def create_client(self, **kwargs: Any) -> BaseClient:
        """
        Create and return a boto3 S3 client configured for MinIO using
        the configured Airflow connection / S3Hook.
        """
        hook = S3Hook(aws_conn_id=kwargs.get("aws_conn_id", self._aws_conn_id))
        return hook.get_conn()

    def delete_client(self, client: Any) -> None:
        """
        Dispose of / close the given storage client instance.

        boto3 S3 clients do not require explicit close, but this method
        exists to satisfy the interface and for future extension.
        """
        # No-op for boto3 S3 client; kept for interface symmetry.
        return None