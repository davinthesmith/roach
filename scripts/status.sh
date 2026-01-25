#!/bin/bash
# Check the health and status of ROACH services

set -e

echo "🔍 ROACH System Status"
echo "===================="
echo ""

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running"
    exit 1
fi

echo "✅ Docker is running"
echo ""

# Check running containers
echo "📦 Running Containers:"
echo "--------------------"
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml ps

echo ""
echo "🏥 Health Checks:"
echo "----------------"

# Function to check container health
check_health() {
    local container=$1
    local name=$2
    
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        local health=$(docker inspect --format='{{.State.Health.Status}}' $container 2>/dev/null || echo "no healthcheck")
        local status=$(docker inspect --format='{{.State.Status}}' $container)
        
        if [ "$health" = "healthy" ]; then
            echo "  ✅ $name: $status (healthy)"
        elif [ "$health" = "no healthcheck" ]; then
            if [ "$status" = "running" ]; then
                echo "  ⚠️  $name: $status (no healthcheck)"
            else
                echo "  ❌ $name: $status"
            fi
        else
            echo "  ⚠️  $name: $status ($health)"
        fi
    else
        echo "  ❌ $name: not running"
    fi
}

check_health "roach-zookeeper" "Zookeeper"
check_health "roach-kafka" "Kafka"
check_health "roach-kafka-ui" "Kafka UI"
check_health "roach-postgres" "PostgreSQL"
check_health "roach-weather-publish" "Weather Publisher"
check_health "roach-weather-sql" "Weather SQL"

echo ""
echo "🌐 Access Points:"
echo "----------------"
echo "  Kafka UI:      http://localhost:8080"
echo "  Kafka Broker:  localhost:9092 (external)"
echo "  Kafka Broker:  kafka:29092 (internal)"
echo "  PostgreSQL:    localhost:5432 (user: roach, db: roach)"
echo ""

# Check Kafka topics if Kafka is running
if docker ps --format '{{.Names}}' | grep -q "^roach-kafka$"; then
    echo "📋 Kafka Topics:"
    echo "---------------"
    docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092 2>/dev/null || echo "  ⚠️  Could not list topics (Kafka may still be starting)"
    echo ""
fi

# Check PostgreSQL database stats if running
if docker ps --format '{{.Names}}' | grep -q "^roach-postgres$"; then
    echo "🗄️  Database Stats:"
    echo "-----------------"
    docker exec roach-postgres psql -U roach -d roach -t -c "
        SELECT 
            'Devices: ' || COUNT(*) FROM devices
        UNION ALL
        SELECT 
            'Tags: ' || COUNT(*) FROM tags
        UNION ALL
        SELECT 
            'Records (numeric): ' || COUNT(*) FROM records_numeric
        UNION ALL
        SELECT 
            'Records (text): ' || COUNT(*) FROM records_text
        UNION ALL
        SELECT 
            'Records (null): ' || COUNT(*) FROM records_null
        UNION ALL
        SELECT 
            'Orphaned messages: ' || COUNT(*) FROM orphaned_messages WHERE NOT reprocessed;
    " 2>/dev/null | sed 's/^/  /' || echo "  ⚠️  Database not ready yet"
    echo ""
fi

# Check disk usage
echo "💾 Disk Usage:"
echo "-------------"
if [ -d "./data" ]; then
    du -sh ./data/* 2>/dev/null || echo "  No data yet"
else
    echo "  No data directory yet"
fi

echo ""
echo "💡 Quick Commands:"
echo "-----------------"
echo "  View logs:        ./scripts/logs.sh [service]"
echo "  Restart service:  ./scripts/restart.sh <service>"
echo "  Stop all:         ./scripts/stop-all.sh"
echo "  DB orphans:       ./scripts/db/reload-orphans.sh"
