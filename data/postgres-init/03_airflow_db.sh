#!/usr/bin/env bash
# Creates the Airflow metadata database alongside the ERP database.
# This script runs once on first postgres container initialization (empty data dir).
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-SQL
    CREATE DATABASE airflow;
    GRANT ALL PRIVILEGES ON DATABASE airflow TO $POSTGRES_USER;
SQL

echo "airflow database created."
