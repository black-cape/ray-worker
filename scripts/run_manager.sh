#!/bin/bash

set -e

echo "Checking Kafka and Zookeeper Status"
  
until timeout 2s curl -v telnet://$KAFKA_BROKER 2>&1 | grep "Connected to"; do
  >&2 echo "Kafka and Zookeeper are unavailable - sleeping"
  sleep 1
done
  
>&2 echo "Kafka and Zookeeper are up"

echo "Checking Minio Status"
  
until curl "${MINIO_HOST}:${MINIO_PORT}"; do
  >&2 echo "Minio is unavailable - sleeping"
  sleep 1
done
  
>&2 echo "Minio is up"

ray start --head --port $RAY_HEAD_PORT \
    --node-manager-port $RAY_NODE_MANAGER_PORT --object-manager-port $RAY_OBJECT_MANAGER_PORT \
    --min-worker-port $RAY_MIN_WORKER_PORT --max-worker-port $RAY_MAX_WORKER_PORT

uvicorn --host 0.0.0.0 --port 8002 etl.app_manager:app
