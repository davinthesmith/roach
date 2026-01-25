#!/bin/bash
# Run weatherlink-backfill-to-kafka with proper compose files
# Rebuilds the image to ensure latest code is used

docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml build weatherlink-backfill-to-kafka
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml run --rm weatherlink-backfill-to-kafka "$@"
