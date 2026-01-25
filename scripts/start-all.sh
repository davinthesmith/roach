#!/bin/bash
# Start all services (infrastructure + application services)
# Usage: ./start-all.sh [build]
#   build - Rebuild containers before starting

set -e

# Check if build argument is provided
BUILD_FLAG=""
if [ "$1" = "build" ]; then
    echo "🔨 Building containers before starting..."
    BUILD_FLAG="--build"
fi

echo "🚀 Starting ROACH Infrastructure and Services..."
echo ""

docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d $BUILD_FLAG

echo ""
echo "✅ All services started!"
echo "   Kafka UI: http://localhost:8080"
echo "   Kafka Broker: localhost:9092"
echo ""
echo "Running services:"
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml ps
echo ""
echo "To view logs: docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml logs -f"
echo "To stop: docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down"
