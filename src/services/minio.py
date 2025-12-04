from typing import Any, Mapping, Optional

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.client import BaseClient

from interfaces.storage_repo import (
    ObjectRepoCRUDInterface,
    ObjectRepoManagerInterface,
)
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


class MinIOObjectRepo(ObjectRepoCRUDInterface):
    """
    CRUD repository for objects stored in a MinIO bucket.

    This class does NOT create or destroy the underlying client. It
    only receives an existing client instance.
    """

    def __init__(self, client: BaseClient, bucket_name: str) -> None:
        self._client = client
        self._bucket = bucket_name

    def create(self, item: Mapping[str, Any]) -> Any:
        """
        Create a new object in the bucket.

        Expected keys in item:
          - "key": object key (path) [required]
          - "body": bytes or a file-like object [required]
          - "extra_args": optional dict of extra arguments for put_object
        """
        key = item["key"]
        body = item["body"]
        extra_args = item.get("extra_args") or {}

        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            **extra_args,
        )
        return key

    def read(self, item_id: Any) -> Optional[Mapping[str, Any]]:
        """
        Retrieve a single object by key.

        Returns a mapping with at least:
          - "key"
          - "body" (bytes)
          - "content_type" (if present)
        or None if the object does not exist.
        """
        key = str(item_id)
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
        except self._client.exceptions.NoSuchKey:
            return None

        body_bytes = response["Body"].read()
        return {
            "key": key,
            "body": body_bytes,
            "content_type": response.get("ContentType"),
            "metadata": response.get("Metadata", {}),
        }

    def update(self, 
               item_id: Any, 
               updates: Mapping[str, Any]
               ) -> Mapping[str, Any]:
        """
        Update an existing object.

        Simplest approach: upload a new object with the same key.
        `updates` is expected to contain at least "body" and
        optionally "extra_args".
        """
        key = str(item_id)
        body = updates["body"]
        extra_args = updates.get("extra_args") or {}

        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body,
            **extra_args,
        )
        return {"key": key, "body": body, "extra_args": extra_args}

    def delete(self, item_id: Any) -> None:
        """Delete an object from the bucket by key."""
        key = str(item_id)
        self._client.delete_object(Bucket=self._bucket, Key=key)


class MinIOObjectRepoManager(ObjectRepoManagerInterface):
    """Manager for creating and deleting MinIO buckets."""

    def __init__(self, client: BaseClient) -> None:
        self._client = client

    def create_repo(
        self,
        repo_id: str,
        *,
        metadata: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Create a new bucket in MinIO."""
        create_kwargs: dict[str, Any] = {"Bucket": repo_id}
        # MinIO can accept additional configuration via metadata if needed
        if metadata:
            create_kwargs.update(metadata)  # type: ignore[arg-type]
        self._client.create_bucket(**create_kwargs)

    def delete_repo(self, repo_id: str, *, force: bool = False) -> None:
        """Delete a bucket from MinIO.

        If ``force`` is True, attempts to remove all objects before
        deleting the bucket.
        """
        if force:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=repo_id):
                contents = page.get("Contents") or []
                if not contents:
                    continue
                objects = [{"Key": obj["Key"]} for obj in contents]
                self._client.delete_objects(
                    Bucket=repo_id,
                    Delete={"Objects": objects},
                )

        self._client.delete_bucket(Bucket=repo_id)
