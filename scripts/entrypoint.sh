#!/bin/bash
set -e

echo "Waiting for postgres..."

# wait until Postgres responds
until pg_isready -h postgres -p 5432 -U airflow; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 2
done

echo "Postgres is up - continuing..."

# Initialize the database if not already
airflow db upgrade

# Start webserver
exec airflow webserver
