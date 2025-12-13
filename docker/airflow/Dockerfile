# Custom Airflow image with preinstalled Python dependencies
ARG AIRFLOW_BASE_IMAGE=apache/airflow:latest
FROM ${AIRFLOW_BASE_IMAGE}

# Mirror the additional requirements used in compose, but bake them in so runtime
# doesn't try to install via _PIP_ADDITIONAL_REQUIREMENTS.
ARG EXTRA_PIP=" \
    apache-airflow-providers-amazon[s3fs] \
    pytest \
    pytest-cov \
    mlflow \
    qdrant-client \
    pyspark \
    ipython \
    ipykernel \
    "

USER root
# Install JDK for PySpark JVM and clean up apt cache.
RUN set -euo pipefail && \
    apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
# Ensure user-level bin path is available for tools like ipython/ipykernel.
ENV PATH="/home/airflow/.local/bin:${PATH}"
RUN set -euo pipefail && \
    pip install --no-cache-dir --user ${EXTRA_PIP}
