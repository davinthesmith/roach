#!/bin/bash
# Stop all ROACH services (infrastructure + applications)
# Usage: ./stop-all.sh [clean]
#   clean - Remove volumes to ensure fresh start (fixes cluster ID mismatches)

set -e

echo "🛑 Stopping all ROACH services..."

# Check if clean flag is provided
if [ "$1" = "clean" ]; then
    echo "🧹 Removing volumes for clean restart..."
    docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
    echo "✅ All services stopped and volumes removed!"
else
    docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down
    echo "✅ All services stopped!"
    echo ""
    echo "💡 To remove volumes and ensure clean start: ./stop-all.sh clean"
fi
