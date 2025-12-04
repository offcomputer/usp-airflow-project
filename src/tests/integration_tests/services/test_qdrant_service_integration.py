from services.qdrant import QdrantDBClient


def test_qdrant_connection_integration() -> None:
    """Ensure we can connect to the Qdrant instance using the service client."""
    client_factory = QdrantDBClient(qdrant_conn_id="qdrant_db")
    client = client_factory.create_client()
    try:
        # Listing collections is sufficient to validate connectivity
        collections = client.get_collections()
        assert hasattr(collections, "collections")
    finally:
        client_factory.delete_client(client)

