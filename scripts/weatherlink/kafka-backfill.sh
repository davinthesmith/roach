#!/bin/bash
# Run weatherlink-kafka-backfill with proper compose files
# Rebuilds the image to ensure latest code is used

docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml build weatherlink-kafka-backfill
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml run --rm weatherlink-kafka-backfill "$@"
