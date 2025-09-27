#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# Set a timestamp for unique backup filenames
TIMESTAMP=$(date +%F-%H%M%S)
PG_USER="postgres"

echo "--- Starting Backup Process ---"

# --- 1. Backup PostgreSQL Data Warehouse ---
echo "Backing up PostgreSQL DWH..."
PG_BACKUP_FILE="./backups/postgres/dwh_dump_${TIMESTAMP}.sql"

# Target the container directly by its name, 'postgres-warehouse'.
docker exec postgres-warehouse sh -c "pg_dumpall -U ${PG_USER}" > ${PG_BACKUP_FILE}
echo "PostgreSQL backup created at ${PG_BACKUP_FILE}"


# --- 2. Backup HDFS Data Lake ---
echo "Backing up HDFS Data Lake..."
HDFS_BACKUP_PATH="./backups/hdfs/hdfs_backup_${TIMESTAMP}"

# These commands also use the basic 'docker exec' and 'docker cp'.
docker exec namenode hdfs dfs -get /user /tmp/hdfs_backup_${TIMESTAMP}
docker cp namenode:/tmp/hdfs_backup_${TIMESTAMP} ./backups/hdfs/
docker exec namenode rm -r /tmp/hdfs_backup_${TIMESTAMP}
echo "HDFS backup created at ${HDFS_BACKUP_PATH}"

echo "--- Backup Process Finished ---"