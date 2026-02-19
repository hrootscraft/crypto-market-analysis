#!/bin/bash
set -e

echo "Installing PostgreSQL driver..."
pip install psycopg2-binary --target /app/.venv/lib/python3.10/site-packages/ --quiet

echo "Running Superset DB migrations..."
superset db upgrade

echo "Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin || true

echo "Initializing Superset..."
superset init

echo "Adding crypto warehouse database connection..."
superset set-database-uri \
  --database-name "Crypto Warehouse" \
  --uri "postgresql://crypto_user:crypto_pass@warehouse:5432/crypto" || true

echo "Superset init complete."
