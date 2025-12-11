from unittest.mock import MagicMock, patch

from services.qdrant import QdrantDBClient


@patch("services.qdrant.QdrantClient")
@patch("services.qdrant.BaseHook")
def test_qdrant_client_uses_airflow_connection(
    mock_base_hook_cls, mock_qdrant_client_cls
) -> None:
    mock_conn = MagicMock()
    mock_conn.get_uri.return_value = "http://qdrant:6333"
    mock_base_hook_cls.get_connection.return_value = mock_conn

    client_factory = QdrantDBClient(qdrant_conn_id="qdrant_db")
    client = client_factory.create_client()

    mock_base_hook_cls.get_connection.assert_called_once_with("qdrant_db")
    mock_conn.get_uri.assert_called_once_with()
    mock_qdrant_client_cls.assert_called_once_with(url="http://qdrant:6333")
    assert client is mock_qdrant_client_cls.return_value


def test_qdrant_client_delete_closes_client_if_supported() -> None:
    client_factory = QdrantDBClient()
    mock_client = MagicMock()

    client_factory.delete_client(mock_client)

    mock_client.close.assert_called_once_with()

