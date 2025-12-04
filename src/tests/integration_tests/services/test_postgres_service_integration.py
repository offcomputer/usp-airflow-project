from services.postgres import PostgresDBClient


def test_postgres_airflow_connection_integration() -> None:
    """Ensure we can connect to the Airflow DB using the service client."""
    client_factory = PostgresDBClient(postgres_conn_id="postgres_airflow")
    conn = client_factory.create_client()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            row = cur.fetchone()
            assert row and row[0] == 1
    finally:
        client_factory.delete_client(conn)


def test_postgres_mlflow_connection_integration() -> None:
    """Ensure we can connect to the MLflow DB using the service client."""
    client_factory = PostgresDBClient(postgres_conn_id="postgres_mlflow")
    conn = client_factory.create_client()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            row = cur.fetchone()
            assert row and row[0] == 1
    finally:
        client_factory.delete_client(conn)
