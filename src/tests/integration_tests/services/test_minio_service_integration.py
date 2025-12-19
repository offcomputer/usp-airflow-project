import uuid

from services.minio import MinIOObjectClient, MinIOObjectRepository


def test_minio_client_integration() -> None:
    """Ensure we can obtain a MinIO S3 client and talk to the endpoint."""
    client_factory = MinIOObjectClient(aws_conn_id="minio_s3")
    client = client_factory.create_client()

    # list_buckets is enough to validate connectivity and credentials
    response = client.list_buckets()
    assert "Buckets" in response


def test_minio_repository_crud_integration() -> None:
    """
    Validate CRUD operations using the MinIOObjectRepository against a real
    MinIO endpoint. Expects a bucket named \"tests\" to exist.
    """
    client_factory = MinIOObjectClient(aws_conn_id="minio_s3")
    client = client_factory.create_client()
    repo = MinIOObjectRepository(client, bucket="tests", prefix="integration")

    object_id = f"{uuid.uuid4()}.txt"
    created_key = None
    try:
        created_key = repo.create(
            {
                "id": object_id,
                "body": b"hello",
                "metadata": {"stage": "create"},
                "content_type": "text/plain",
            }
        )
        assert created_key == object_id

        fetched = repo.read(object_id)
        assert fetched is not None
        assert fetched["body"] == b"hello"
        assert fetched["metadata"].get("stage") == "create"
        assert fetched["content_type"] == "text/plain"

        updated = repo.update(
            object_id,
            {
                "body": b"updated",
                "metadata": {"stage": "update"},
                "content_type": "text/plain",
            },
        )
        assert updated["body"] == b"updated"
        assert updated["metadata"]["stage"] == "update"

        fetched_after_update = repo.read(object_id)
        assert fetched_after_update is not None
        assert fetched_after_update["body"] == b"updated"
        assert fetched_after_update["metadata"].get("stage") == "update"
    finally:
        if created_key is not None:
            repo.delete(created_key)
