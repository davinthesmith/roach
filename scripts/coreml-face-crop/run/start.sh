#!/bin/bash
# Start coreml-face-crop as a background daemon
# Usage: ./scripts/coreml-face-crop/run/start.sh
# Logs: data/logs/coreml-face-crop.log
# PID:  data/logs/coreml-face-crop.pid

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

LOG_DIR="data/logs"
LOG_FILE="$LOG_DIR/coreml-face-crop.log"
PID_FILE="$LOG_DIR/coreml-face-crop.pid"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "⚠️  coreml-face-crop is already running (PID $PID)"
        echo "   Stop first: ./scripts/coreml-face-crop/run/stop.sh"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

# Ensure built
if [ ! -d services/coreml-face-crop/.build ]; then
    echo "📦 Building first..."
    ./scripts/coreml-face-crop/build/build.sh
fi

BIN="$ROACH_ROOT/services/coreml-face-crop/.build/release/CoreMLFaceCrop"
[ -x "$BIN" ] || BIN="$ROACH_ROOT/services/coreml-face-crop/.build/debug/CoreMLFaceCrop"
[ -x "$BIN" ] || { echo "No built binary; run ./scripts/coreml-face-crop/build/build.sh" >&2; exit 1; }

mkdir -p "$LOG_DIR"

echo "🚀 Starting coreml-face-crop daemon..."
nohup "$BIN" >> "$LOG_FILE" 2>&1 &
DPID=$!
echo "$DPID" > "$PID_FILE"

echo "✅ coreml-face-crop started (PID $DPID)"
echo "   Logs: $LOG_FILE"
echo "   Stop: ./scripts/coreml-face-crop/run/stop.sh"
