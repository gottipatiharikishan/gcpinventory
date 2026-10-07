#!/bin/bash

PENDING=$(PGPASSWORD=$PGPASSWORD psql \
-h 127.0.0.1 \
-p 5432 \
-U gcssync \
-d gcssync \
-t -c "
SELECT COUNT(*)
FROM sync_batches
WHERE status='PENDING';
" | xargs)

if [ "$PENDING" -lt 100 ]; then
    docker-compose -f /opt/gcs-azure-sync-v2/docker-compose.yml up -d --scale worker=10

elif [ "$PENDING" -lt 1000 ]; then
    docker-compose -f /opt/gcs-azure-sync-v2/docker-compose.yml up -d --scale worker=25

elif [ "$PENDING" -lt 5000 ]; then
    docker-compose -f /opt/gcs-azure-sync-v2/docker-compose.yml up -d --scale worker=50

else
    docker-compose -f /opt/gcs-azure-sync-v2/docker-compose.yml up -d --scale worker=100

fi
