from unittest.mock import MagicMock, patch
from services.mlflow import MlflowClient


@patch("services.mlflow.mlflow")
@patch("services.mlflow.BaseHook")
def test_mlflow_client_uses_airflow_connection(mock_base_hook_cls, 
                                               mock_mlflow_mod):
    mock_conn = MagicMock()
    mock_conn.get_uri.return_value = "http://mlflow-server:5000"
    mock_base_hook_cls.get_connection.return_value = mock_conn

    client_factory = MlflowClient(mlflow_conn_id="mlflow_tracking")
    client = client_factory.create_client()

    mock_base_hook_cls.get_connection.assert_called_once_with(
        "mlflow_tracking")
    mock_conn.get_uri.assert_called_once_with()
    mock_mlflow_mod.set_tracking_uri.assert_called_once_with(
        "http://mlflow-server:5000"
    )
    mock_mlflow_mod.MlflowClient.assert_called_once_with(
        tracking_uri="http://mlflow-server:5000"
    )
    assert client is mock_mlflow_mod.MlflowClient.return_value


def test_mlflow_client_delete_is_noop():
    client_factory = MlflowClient()
    mock_client = MagicMock()

    # Should not raise and not require any specific method
    client_factory.delete_client(mock_client)

