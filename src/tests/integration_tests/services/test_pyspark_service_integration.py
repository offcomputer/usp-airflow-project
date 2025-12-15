import logging
import os
from services.pyspark import PySparkClient


def test_pyspark_client_integration() -> None:
    """Ensure we can obtain a SparkSession and run a tiny job against the Spark service."""
    # Silence py4j logging to avoid teardown warnings when the stream closes.
    logging.getLogger("py4j").handlers = [logging.NullHandler()]
    logging.getLogger("py4j").propagate = False

    # Allow overriding master for CI, but default to using the Airflow spark_default connection.
    override_master = os.getenv("SPARK_TEST_MASTER")
    client_factory = PySparkClient(
        master=override_master,
        app_name="integration-test",
    )
    session = client_factory.create_client()
    try:
        sc = session.sparkContext
        assert sc.defaultParallelism > 0
        assert sc.master
        if not override_master:
            assert sc.master.startswith("spark://")

        data = sc.parallelize(range(10))
        assert data.sum() == 45
    finally:
        client_factory.delete_client(session)
