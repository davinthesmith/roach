#!/bin/bash
# Delete all weather Kafka topics (preserves internal topics)

set -e

CONTAINER="roach-kafka"
BOOTSTRAP_SERVER="localhost:29092"

if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "Kafka is not running."
    exit 1
fi

topics=$(docker exec "$CONTAINER" kafka-topics --list --bootstrap-server "$BOOTSTRAP_SERVER" | grep '^weather\.' || true)

if [ -z "$topics" ]; then
    echo "No weather topics found to delete."
    exit 0
fi

echo "Deleting Kafka topics:"
echo "$topics" | sed 's/^/  - /'

while IFS= read -r topic; do
    [ -z "$topic" ] && continue
    docker exec "$CONTAINER" kafka-topics --delete --topic "$topic" --bootstrap-server "$BOOTSTRAP_SERVER"
done <<< "$topics"

echo "Waiting for deletions to complete..."
sleep 2

remaining=$(docker exec "$CONTAINER" kafka-topics --list --bootstrap-server "$BOOTSTRAP_SERVER" | grep '^weather\.' || true)

if [ -z "$remaining" ]; then
    echo "All weather topics deleted."
else
    echo "Some topics remain:"
    echo "$remaining" | sed 's/^/  - /'
fi
