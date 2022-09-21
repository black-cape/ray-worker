#!/bin/bash

set -e

echo "Checking Kafka and Zookeeper Status"
  
until timeout 2s curl -v telnet://$KAFKA_BROKER 2>&1 | grep "Connected to"; do
  >&2 echo "Kafka and Zookeeper are unavailable - sleeping"
  sleep 1
done
  
>&2 echo "Kafka and Zookeeper are up"

echo "Checking Minio Status"
  
until curl $MINIO_HOST; do
  >&2 echo "Minio is unavailable - sleeping"
  sleep 1
done
  
>&2 echo "Minio is up"

uvicorn --host 0.0.0.0 --port 8002 etl.app_manager:app
