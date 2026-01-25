#!/bin/bash
# Stop all ROACH services (infrastructure + applications)

set -e

echo "🛑 Stopping all ROACH services..."

docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down

echo ""
echo "✅ All services stopped!"
echo ""
echo "To remove volumes as well: docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v"
