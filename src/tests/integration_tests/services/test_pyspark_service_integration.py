import logging
from services.pyspark import PySparkClient


def test_pyspark_client_integration() -> None:
    """Ensure we can obtain a SparkSession and run a tiny job."""
    # Silence py4j logging to avoid teardown warnings when the stream closes.
    logging.getLogger("py4j").handlers = [logging.NullHandler()]
    logging.getLogger("py4j").propagate = False

    client_factory = PySparkClient(master="local[*]", 
                                   app_name="integration-test")
    session = client_factory.create_client()
    try:
        sc = session.sparkContext
        assert sc.defaultParallelism > 0
        assert sc.master

        data = sc.parallelize(range(10))
        assert data.sum() == 45
    finally:
        client_factory.delete_client(session)
