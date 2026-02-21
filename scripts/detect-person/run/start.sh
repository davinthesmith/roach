#!/bin/bash
# Start detect-person as a background daemon
# Usage: ./scripts/detect-person/run/start.sh
# Logs: data/logs/detect-person.log
# PID:  data/logs/detect-person.pid

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

LOG_DIR="data/logs"
LOG_FILE="$LOG_DIR/detect-person.log"
PID_FILE="$LOG_DIR/detect-person.pid"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "⚠️  detect-person is already running (PID $PID)"
        echo "   Stop first: ./scripts/detect-person/run/stop.sh"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

# Ensure built
if [ ! -d services/detect-person/.build ]; then
    echo "📦 Building first..."
    ./scripts/detect-person/build/build.sh
fi

mkdir -p "$LOG_DIR"

echo "🚀 Starting detect-person daemon..."
cd services/detect-person
nohup swift run DetectPerson detect >> "../../$LOG_FILE" 2>&1 &
DPID=$!
cd ../..
echo "$DPID" > "$PID_FILE"

echo "✅ detect-person started (PID $DPID)"
echo "   Logs: $LOG_FILE"
echo "   Stop: ./scripts/detect-person/run/stop.sh"
