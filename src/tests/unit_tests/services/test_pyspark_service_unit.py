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


def test_pyspark_client_delete_stops_session():
    client_factory = PySparkClient()
    mock_session = MagicMock()

    client_factory.delete_client(mock_session)

    mock_session.stop.assert_called_once_with()
