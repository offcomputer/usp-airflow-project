from services.minio import MinIOObjectClient


def test_minio_client_integration() -> None:
    """Ensure we can obtain a MinIO S3 client and talk to the endpoint."""
    client_factory = MinIOObjectClient(aws_conn_id="minio_s3")
    client = client_factory.create_client()

    # list_buckets is enough to validate connectivity and credentials
    response = client.list_buckets()
    assert "Buckets" in response
