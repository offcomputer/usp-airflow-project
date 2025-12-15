# Custom Airflow image with preinstalled Python dependencies
FROM apache/airflow:3.1.3

# Mirror the additional requirements used in compose, but bake them in so runtime
# doesn't try to install via _PIP_ADDITIONAL_REQUIREMENTS.
ARG EXTRA_PIP=" \
    apache-airflow-providers-amazon[s3fs]==9.16.0 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-daskexecutor \
    pytest==9.0.2 \
    pytest-cov==7.0.0 \
    mlflow==3.7.0 \
    qdrant-client==1.16.2 \
    pyspark==4.0.1 \
    ipython==9.8.0 \
    ipykernel==7.1.0 \
    "

USER root

RUN set -euo pipefail && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN set -euo pipefail && \
    pip install --no-cache-dir --user ${EXTRA_PIP}

# Ensure user-local bin is on PATH for installed Python tools
ENV PATH="/home/airflow/.local/bin:${PATH}"
