#!/bin/bash

set -e

echo "Checking Kafka Status"

until timeout 2s curl -v telnet://$KAFKA_BOOTSTRAP_SERVER 2>&1 | grep "Connected to"; do
  >&2 echo "Kafka is unavailable - sleeping"
  sleep 1
done

>&2 echo "Kafka is up"

echo "Checking Minio Status"

until curl "${MINIO_HOST}:${MINIO_PORT}"; do
  >&2 echo "Minio is unavailable - sleeping"
  sleep 1
done

>&2 echo "Minio is up"

ray start --head --port $RAY_HEAD_PORT \
    --node-manager-port $RAY_NODE_MANAGER_PORT --object-manager-port $RAY_OBJECT_MANAGER_PORT \
    --min-worker-port $RAY_MIN_WORKER_PORT --max-worker-port $RAY_MAX_WORKER_PORT \
    --disable-usage-stats --include-dashboard false

alembic upgrade head && poetry run python lib_ray_worker/entrypoint.py
