from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from services.minio import MinIOObjectClient, MinIOObjectRepository


@patch("services.minio.S3Hook")
def test_minio_object_client_uses_airflow_connection(mock_hook_cls):
    mock_hook = MagicMock()
    mock_client = MagicMock()
    mock_hook.get_conn.return_value = mock_client
    mock_hook_cls.return_value = mock_hook

    client_factory = MinIOObjectClient(aws_conn_id="minio_s3")
    client = client_factory.create_client()

    mock_hook_cls.assert_called_once_with(aws_conn_id="minio_s3")
    mock_hook.get_conn.assert_called_once_with()
    assert client is mock_client


def test_minio_object_repository_create_puts_object_with_prefix():
    mock_client = MagicMock()
    repo = MinIOObjectRepository(mock_client, bucket="bucket", prefix="base")

    key = repo.create(
        {"id": "file.txt", "body": b"data", "metadata": {"a": "b"}, "content_type": "text/plain"}
    )

    assert key == "file.txt"
    mock_client.put_object.assert_called_once_with(
        Body=b"data",
        Bucket="bucket",
        Key="base/file.txt",
        Metadata={"a": "b"},
        ContentType="text/plain",
    )


def test_minio_object_repository_read_returns_object_dict():
    mock_body = MagicMock()
    mock_body.read.return_value = b"payload"
    mock_client = MagicMock()
    mock_client.get_object.return_value = {
        "Body": mock_body,
        "Metadata": {"x": "y"},
        "ContentType": "application/octet-stream",
        "ETag": '"etag123"',
    }
    repo = MinIOObjectRepository(mock_client, bucket="bucket")

    result = repo.read("obj-id")

    mock_client.get_object.assert_called_once_with(Bucket="bucket", Key="obj-id")
    assert result == {
        "id": "obj-id",
        "body": b"payload",
        "metadata": {"x": "y"},
        "content_type": "application/octet-stream",
        "etag": '"etag123"',
    }


def test_minio_object_repository_read_returns_none_when_missing_key():
    mock_client = MagicMock()
    mock_client.get_object.side_effect = ClientError(
        error_response={"Error": {"Code": "NoSuchKey", "Message": "not found"}},
        operation_name="GetObject",
    )
    repo = MinIOObjectRepository(mock_client, bucket="bucket")

    result = repo.read("missing")

    assert result is None
    mock_client.get_object.assert_called_once_with(Bucket="bucket", Key="missing")


def test_minio_object_repository_update_overwrites_and_returns_metadata():
    mock_client = MagicMock()
    mock_client.put_object.return_value = {"ETag": '"new"'}
    repo = MinIOObjectRepository(mock_client, bucket="bucket", prefix="pfx")

    updated = repo.update(
        "file.txt", {"body": b"new", "metadata": {"k": "v"}, "content_type": "bin"}
    )

    mock_client.put_object.assert_called_once_with(
        Body=b"new",
        Bucket="bucket",
        Key="pfx/file.txt",
        Metadata={"k": "v"},
        ContentType="bin",
    )
    assert updated == {
        "id": "file.txt",
        "body": b"new",
        "metadata": {"k": "v"},
        "content_type": "bin",
        "etag": '"new"',
    }


def test_minio_object_repository_delete_removes_object():
    mock_client = MagicMock()
    repo = MinIOObjectRepository(mock_client, bucket="bucket", prefix="pfx")

    repo.delete("key")

    mock_client.delete_object.assert_called_once_with(
        Bucket="bucket", Key="pfx/key"
    )


def test_minio_object_repository_accepts_mapping_with_key_field():
    mock_client = MagicMock()
    repo = MinIOObjectRepository(mock_client, bucket="bucket")

    repo.create({"key": "alternate", "body": b"x"})

    mock_client.put_object.assert_called_once_with(
        Body=b"x", Bucket="bucket", Key="alternate"
    )


def test_minio_object_repository_requires_body_on_create_and_update():
    repo = MinIOObjectRepository(MagicMock(), bucket="bkt")

    try:
        repo.create({"id": "missing"})  # type: ignore[arg-type]
    except ValueError as exc:
        assert "`body` is required" in str(exc)
    else:  # pragma: no cover - defensive
        assert False

    try:
        repo.update("key", {})  # type: ignore[arg-type]
    except ValueError as exc:
        assert "`body` is required" in str(exc)
    else:  # pragma: no cover - defensive
        assert False
