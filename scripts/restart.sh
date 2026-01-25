#!/bin/bash
# Restart a specific service or all services

if [ -z "$1" ]; then
    echo "🔄 Restarting all services..."
    docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml restart
else
    echo "🔄 Restarting: $1"
    docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml restart "$1"
fi

echo "✅ Restart complete!"
