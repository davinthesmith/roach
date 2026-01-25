#!/bin/bash
# Run weatherlink-backfill with proper compose files

docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml run --rm weatherlink-backfill "$@"
