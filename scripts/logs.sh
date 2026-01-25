#!/bin/bash
# View logs for all services or a specific service

if [ -z "$1" ]; then
    echo "📋 Viewing logs for all services..."
    docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs -f
else
    echo "📋 Viewing logs for: $1"
    docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs -f "$1"
fi
