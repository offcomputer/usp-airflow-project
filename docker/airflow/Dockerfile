# Custom Airflow image with preinstalled Python dependencies
ARG AIRFLOW_BASE_IMAGE=apache/airflow:latest
FROM ${AIRFLOW_BASE_IMAGE}

# Mirror the additional requirements used in compose, but bake them in so runtime
# doesn't try to install via _PIP_ADDITIONAL_REQUIREMENTS.
ARG EXTRA_PIP="apache-airflow-providers-amazon[s3fs] pytest pytest-cov mlflow qdrant-client"

RUN set -euo pipefail && \
    pip install --no-cache-dir ${EXTRA_PIP}
