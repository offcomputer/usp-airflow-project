from unittest.mock import MagicMock, patch
from services.pyspark import PySparkClient


@patch("services.pyspark.SparkSession")
def test_pyspark_client_builds_session_with_config(mock_spark_cls):
    mock_builder = MagicMock()
    mock_session = MagicMock()

    # Chainable builder methods
    mock_builder.master.return_value = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session

    mock_spark_cls.builder = mock_builder

    client_factory = PySparkClient(master="local[2]", 
                                   app_name="unit-test", 
                                   configs={"k": "v"})
    session = client_factory.create_client(configs={"extra": "val"})

    mock_builder.master.assert_called_once_with("local[2]")
    mock_builder.appName.assert_called_once_with("unit-test")
    mock_builder.config.assert_any_call("k", "v")
    mock_builder.config.assert_any_call("extra", "val")
    mock_builder.getOrCreate.assert_called_once_with()
    assert session is mock_session


@patch("services.pyspark.BaseHook")
@patch("services.pyspark.SparkSession")
def test_pyspark_client_uses_airflow_connection(mock_spark_cls, mock_base_hook):
    mock_builder = MagicMock()
    mock_session = MagicMock()

    mock_builder.master.return_value = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_session

    mock_spark_cls.builder = mock_builder

    mock_conn = MagicMock()
    mock_conn.get_uri.return_value = "spark://spark:7077"
    mock_conn.extra_dejson = {"spark.submit.deployMode": "client"}
    mock_base_hook.get_connection.return_value = mock_conn

    client_factory = PySparkClient()
    session = client_factory.create_client(configs={"extra": "val"})

    mock_base_hook.get_connection.assert_called_once_with("spark_default")
    mock_builder.master.assert_called_once_with("spark://spark:7077")
    mock_builder.appName.assert_called_once_with("app")
    mock_builder.config.assert_any_call("spark.submit.deployMode", "client")
    mock_builder.config.assert_any_call("extra", "val")
    mock_builder.getOrCreate.assert_called_once_with()
    assert session is mock_session


def test_pyspark_client_delete_stops_session():
    client_factory = PySparkClient()
    mock_session = MagicMock()

    client_factory.delete_client(mock_session)

    mock_session.stop.assert_called_once_with()
