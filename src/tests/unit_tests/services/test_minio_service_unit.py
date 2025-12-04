from unittest.mock import MagicMock, patch
from services.minio import MinIOObjectClient, MinIOObjectRepoManager


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


def test_minio_repo_manager_create_and_delete_repo_force():
    mock_client = MagicMock()
    manager = MinIOObjectRepoManager(client=mock_client)

    # create_repo
    manager.create_repo("test-bucket", metadata={"ACL": "private"})
    mock_client.create_bucket.assert_called_once()
    args, kwargs = mock_client.create_bucket.call_args
    assert kwargs["Bucket"] == "test-bucket"
    assert kwargs["ACL"] == "private"

    # delete_repo with force=True should list 
    # and delete objects then delete bucket
    mock_client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "obj1"}, {"Key": "obj2"}]}
    ]

    manager.delete_repo("test-bucket", force=True)

    mock_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_client.delete_objects.assert_called_once()
    _, del_kwargs = mock_client.delete_objects.call_args
    assert del_kwargs["Bucket"] == "test-bucket"
    assert del_kwargs["Delete"]["Objects"] == [{"Key": "obj1"}, 
                                               {"Key": "obj2"}]
    mock_client.delete_bucket.assert_called_with(Bucket="test-bucket")
