#!/bin/bash
set -e

echo "Applying PostgreSQL tuning parameters from environment variables..."

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" <<EOF
ALTER SYSTEM SET shared_buffers = '${POSTGRES_AIRFLOW_SHARED_BUFFERS}';
ALTER SYSTEM SET max_connections = '${POSTGRES_AIRFLOW_MAX_CONNECTIONS}';
ALTER SYSTEM SET effective_cache_size = '${POSTGRES_AIRFLOW_EFFECTIVE_CACHE_SIZE}';
ALTER SYSTEM SET maintenance_work_mem = '${POSTGRES_AIRFLOW_MAINTENANCE_WORK_MEM}';
EOF

MLFLOW_DB_NAME="${POSTGRES_MLFLOW_DB:-mlflow}"
EXPORTER_USER="${POSTGRES_EXPORTER_USER:-pgmon}"
EXPORTER_PASSWORD="${POSTGRES_EXPORTER_PASSWORD:-pgmon}"

echo "Ensuring ${MLFLOW_DB_NAME} database exists..."
MLFLOW_DB_EXISTS=$(psql -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '${MLFLOW_DB_NAME}';")

if [ "$MLFLOW_DB_EXISTS" != "1" ]; then
  echo "Creating ${MLFLOW_DB_NAME} database..."
  psql -U "$POSTGRES_USER" -d postgres -c "CREATE DATABASE ${MLFLOW_DB_NAME};"
  echo "${MLFLOW_DB_NAME} database created."
else
  echo "${MLFLOW_DB_NAME} database already exists."
fi

echo "Ensuring exporter role ${EXPORTER_USER} exists..."
psql -U "$POSTGRES_USER" -d postgres <<EOF
DO
\$do\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${EXPORTER_USER}') THEN
    CREATE ROLE ${EXPORTER_USER} LOGIN PASSWORD '${EXPORTER_PASSWORD}';
  END IF;
END
\$do\$;
GRANT pg_monitor TO ${EXPORTER_USER};
GRANT pg_read_all_stats TO ${EXPORTER_USER};
EOF
echo "Exporter role ${EXPORTER_USER} ready."

echo "PostgreSQL tuning applied successfully."
