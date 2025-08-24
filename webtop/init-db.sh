#!/bin/bash
set -e

# Wait for PostgreSQL to be ready
until pg_isready -U postgres; do
  echo "Waiting for PostgreSQL to be ready..."
  sleep 2
done

echo "PostgreSQL is ready. Setting up database..."

# Execute the pipeline setup script first
psql -U postgres -d pipeline -f /sql/setup/pipeline.sql

echo "Database setup complete!"
