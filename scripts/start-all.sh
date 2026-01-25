#!/bin/bash
# Start all services (infrastructure + application services)
# Usage: ./start-all.sh [build] [clean]
#   build - Rebuild containers before starting
#   clean - Remove volumes before starting (fixes cluster ID mismatches)
#
# Note: Due to Zookeeper ephemeral node persistence, Kafka may fail health checks
# during initial startup and require a retry. This script handles that automatically.

set -e

# Parse arguments
BUILD_FLAG=""
CLEAN_START=false

for arg in "$@"; do
    case $arg in
        build)
            echo "🔨 Building containers before starting..."
            BUILD_FLAG="--build"
            ;;
        clean)
            CLEAN_START=true
            ;;
    esac
done

# Clean volumes if requested
if [ "$CLEAN_START" = true ]; then
    echo "🧹 Removing volumes for clean start..."
    docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v 2>/dev/null || true
    echo ""
fi

echo "🚀 Starting ROACH Infrastructure and Services..."
echo ""

# Try to start services
if ! docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d $BUILD_FLAG; then
    echo ""
    echo "⚠️  Initial startup failed (likely Kafka health check during Zookeeper session recovery)"
    echo "🔄 Retrying service startup..."
    sleep 3
    docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml up -d
fi

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
echo "To stop and clean volumes: ./stop-all.sh clean"