#!/bin/bash
# Check detect-person status
# Usage: ./scripts/detect-person/run/status.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

echo "🔍 detect-person Status"
echo "======================"
echo ""

PID_FILE="data/logs/detect-person.pid"

# Process status (PID file from start.sh, or launchd)
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        UPTIME=$(ps -o etime= -p "$PID" | xargs)
        echo "  ✅ Running (PID $PID, uptime: $UPTIME)"
    else
        echo "  ❌ Not running (stale PID file)"
    fi
elif launchctl list com.roach.detect-person &>/dev/null; then
    echo "  ✅ Running (launchd)"
else
    echo "  ❌ Not running"
fi
echo ""

# Model status
MODEL_DIR="data/models/detect-person"
if [ -d "$MODEL_DIR/PersonClassifier.mlmodelc" ]; then
    MODEL_DATE=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M" "$MODEL_DIR/PersonClassifier.mlmodelc")
    echo "🧠 Model: $MODEL_DIR/PersonClassifier.mlmodelc (trained $MODEL_DATE)"
else
    echo "🧠 Model: not trained yet"
fi
echo ""

# Training data
TRAIN_DIR="data/train"
if [ -d "$TRAIN_DIR" ]; then
    PERSON_COUNT=$(find "$TRAIN_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | xargs)
    IMAGE_COUNT=$(find "$TRAIN_DIR" -name "*.jpg" -o -name "*.png" | wc -l | xargs)
    echo "📂 Training data: $PERSON_COUNT people, $IMAGE_COUNT images ($TRAIN_DIR)"
else
    echo "📂 Training data: not found ($TRAIN_DIR)"
fi
echo ""

# Watch directory
WATCH_DIR="data/streams/unifi/protect/smart/person"
if [ -d "$WATCH_DIR" ]; then
    CAMERA_COUNT=$(find "$WATCH_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | xargs)
    echo "👁️  Watch directory: $CAMERA_COUNT cameras ($WATCH_DIR)"
else
    echo "👁️  Watch directory: not found ($WATCH_DIR)"
fi
echo ""

# Log file
LOG_FILE="data/logs/detect-person.log"
if [ -f "$LOG_FILE" ]; then
    LOG_SIZE=$(du -sh "$LOG_FILE" | cut -f1 | xargs)
    echo "📋 Log file: $LOG_FILE ($LOG_SIZE)"
else
    echo "📋 Log file: none yet"
fi
echo ""

# Kafka topic check
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q "^roach-kafka$"; then
    echo "📡 Kafka topic (detect.person):"
    docker exec roach-kafka kafka-topics --describe --topic detect.person \
        --bootstrap-server localhost:29092 2>/dev/null \
        | sed 's/^/   /' || echo "   Topic not yet created (created on first message)"
else
    echo "📡 Kafka: not running"
fi
