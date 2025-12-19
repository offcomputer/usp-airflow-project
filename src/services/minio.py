from typing import Any, Mapping, Optional
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from interfaces.clients import ServiceClientInterface
from interfaces.storage_repo import ObjectRepoCRUDInterface


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


class MinIOObjectRepository(ObjectRepoCRUDInterface):
    """CRUD implementation for a MinIO bucket using a boto3-compatible
    client.

    The repository operates against a single bucket/prefix and expects the
    injected client to follow the S3/boto3 API surface (e.g., ``put_object``,
    ``get_object``).
    """

    def __init__(self, client: BaseClient, bucket: str, *, prefix: str = "") -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _format_key(self, item_id: str) -> str:
        return f"{self._prefix}/{item_id}" if self._prefix else item_id

    def _extract_item_id(self, item_id: Any) -> str:
        if isinstance(item_id, Mapping):
            candidate: Optional[Any] = item_id.get("id") or item_id.get("key")
            if candidate is not None:
                return str(candidate)
        if hasattr(item_id, "id"):
            return str(getattr(item_id, "id"))
        return str(item_id)

    def _object_kwargs(self, key: str, mapping: Mapping[str, Any]) -> Mapping[str, Any]:
        payload = {
            "Bucket": self._bucket,
            "Key": self._format_key(key),
        }
        if "metadata" in mapping and mapping["metadata"] is not None:
            payload["Metadata"] = mapping["metadata"]
        if "content_type" in mapping and mapping["content_type"] is not None:
            payload["ContentType"] = mapping["content_type"]
        return payload

    def create(self, item: Mapping[str, Any]) -> Any:
        key = self._extract_item_id(item)
        body = item.get("body")
        if body is None:
            raise ValueError("`body` is required to create an object in MinIO")

        kwargs = self._object_kwargs(key, item)
        self._client.put_object(Body=body, **kwargs)
        return key

    def read(self, item_id: Any) -> Optional[Mapping[str, Any]]:
        key = self._extract_item_id(item_id)
        try:
            response = self._client.get_object(
                Bucket=self._bucket, Key=self._format_key(key)
            )
        except ClientError as exc:  # pragma: no cover - defensive
            error_code = exc.response.get("Error", {}).get("Code")
            if error_code in {"NoSuchKey", "404"}:
                return None
            raise

        body = response["Body"].read()
        return {
            "id": key,
            "body": body,
            "metadata": response.get("Metadata", {}),
            "content_type": response.get("ContentType"),
            "etag": response.get("ETag"),
        }

    def update(self, item_id: Any, updates: Mapping[str, Any]) -> Mapping[str, Any]:
        key = self._extract_item_id(item_id)
        body = updates.get("body")
        if body is None:
            raise ValueError("`body` is required to update an object in MinIO")

        kwargs = self._object_kwargs(key, updates)
        response = self._client.put_object(Body=body, **kwargs)
        return {
            "id": key,
            "body": body,
            "metadata": kwargs.get("Metadata", {}),
            "content_type": kwargs.get("ContentType"),
            "etag": response.get("ETag"),
        }

    def delete(self, item_id: Any) -> None:
        key = self._extract_item_id(item_id)
        self._client.delete_object(Bucket=self._bucket, Key=self._format_key(key))
        return None
