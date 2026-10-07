#!/bin/bash

source /opt/gcs-azure-sync-v2/.env

export PGPASSWORD="$PGPASSWORD"

psql \
-h "$PGHOST" \
-p "$PGPORT" \
-U "$PGUSER" \
-d "$PGDATABASE" <<SQL

UPDATE sync_batches
SET status='PENDING'
WHERE status='PROCESSING'
AND started_at IS NOT NULL
AND started_at < NOW() - INTERVAL '30 minutes';

UPDATE rollback_batches
SET status='PENDING'
WHERE status='PROCESSING'
AND started_at IS NOT NULL
AND started_at < NOW() - INTERVAL '30 minutes';

SQL
