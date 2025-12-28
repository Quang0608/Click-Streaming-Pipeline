#!/bin/bash
# run inside a container that has kafka binaries or on the host if kafka-tools installed
docker exec -it kafka-1 kafka-topics \
  --create \
  --topic tracking_events \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1
echo "Topic job_events created (or already exists)."
