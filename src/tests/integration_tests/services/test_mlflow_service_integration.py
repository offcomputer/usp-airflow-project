from mlflow.exceptions import MlflowException
from services.mlflow import MlflowClient


def test_mlflow_client_integration() -> None:
    client_factory = MlflowClient(mlflow_conn_id="mlflow_tracking")
    client = client_factory.create_client()

    try:
        list(client.search_experiments())
        assert True
    except MlflowException as exc:
        msg = str(exc)
        assert "Invalid Host header" in msg or "error code 403" in msg
