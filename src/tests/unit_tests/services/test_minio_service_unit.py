from unittest.mock import MagicMock, patch
from services.minio import MinIOObjectClient


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
