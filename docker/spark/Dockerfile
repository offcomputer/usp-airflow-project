FROM apache/spark:4.0.1-scala2.13-java21-python3-ubuntu

SHELL ["/bin/bash", "-c"]

USER root

# Align executor Python version with Airflow (Python 3.12) to avoid
# PYTHON_VERSION_MISMATCH between driver and workers. Ubuntu jammy
# image for Spark does not ship Python 3.12 packages, so build from source.
RUN set -euo pipefail \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        wget \
        libssl-dev \
        zlib1g-dev \
        libncurses5-dev \
        libbz2-dev \
        liblzma-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        uuid-dev \
        tk-dev \
    && PY_VER=3.12.7 \
    && cd /tmp \
    && wget -q https://www.python.org/ftp/python/${PY_VER}/Python-${PY_VER}.tgz \
    && tar -xzf Python-${PY_VER}.tgz \
    && cd Python-${PY_VER} \
    && ./configure --enable-optimizations --with-ensurepip=install \
    && make -j"$(nproc)" \
    && make altinstall \
    && ln -sf /usr/local/bin/python3.12 /usr/bin/python3 \
    && ln -sf /usr/local/bin/pip3.12 /usr/bin/pip3 \
    && cd / \
    && rm -rf /tmp/Python-${PY_VER} /tmp/Python-${PY_VER}.tgz \
    && apt-get purge -y --auto-remove \
        build-essential \
        wget \
        libssl-dev \
        zlib1g-dev \
        libncurses5-dev \
        libbz2-dev \
        liblzma-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        uuid-dev \
        tk-dev \
    && rm -rf /var/lib/apt/lists/*

USER spark

ENV PYSPARK_PYTHON=/usr/local/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12
