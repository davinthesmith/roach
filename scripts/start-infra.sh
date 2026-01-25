#!/bin/bash
# Start only the Kafka infrastructure (Zookeeper, Kafka, Kafka UI)

set -e

echo "🚀 Starting ROACH Infrastructure..."
echo "   - Zookeeper"
echo "   - Kafka"
echo "   - Kafka UI"
echo ""

docker compose -f docker-compose.infrastructure.yml up -d

echo ""
echo "✅ Infrastructure started!"
echo "   Kafka UI: http://localhost:8080"
echo "   Kafka Broker: localhost:9092"
echo ""
echo "To view logs: docker compose -f docker-compose.infrastructure.yml logs -f"
echo "To stop: docker compose -f docker-compose.infrastructure.yml down"
