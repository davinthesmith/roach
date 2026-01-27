#!/bin/bash
# Trigger backfill by restarting weatherlink-kafka with KAFKA_BACKFILL_ENABLED=true
# Uses docker compose so .env is respected; inline env overrides .env if present.

KAFKA_BACKFILL_ENABLED=true docker compose \
  -f docker-compose.infrastructure.yml -f docker-compose.yml \
  up -d --no-deps --build --force-recreate weatherlink-kafka
