#!/bin/bash
# Start coreml-smart-crop as a background daemon
# Usage: ./scripts/coreml-smart-crop/run/start.sh
# Logs: data/logs/coreml-smart-crop.log
# PID:  data/logs/coreml-smart-crop.pid

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

LOG_DIR="data/logs"
LOG_FILE="$LOG_DIR/coreml-smart-crop.log"
PID_FILE="$LOG_DIR/coreml-smart-crop.pid"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "⚠️  coreml-smart-crop is already running (PID $PID)"
        echo "   Stop first: ./scripts/coreml-smart-crop/run/stop.sh"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

# Ensure built
if [ ! -d services/coreml-smart-crop/.build ]; then
    echo "📦 Building first..."
    ./scripts/coreml-smart-crop/build/build.sh
fi

BIN="$ROACH_ROOT/services/coreml-smart-crop/.build/release/CoreMLSmartCrop"
[ -x "$BIN" ] || BIN="$ROACH_ROOT/services/coreml-smart-crop/.build/debug/CoreMLSmartCrop"
[ -x "$BIN" ] || { echo "No built binary; run ./scripts/coreml-smart-crop/build/build.sh" >&2; exit 1; }

mkdir -p "$LOG_DIR"

# Start with a fresh log so old "model not found" lines from previous runs don't keep showing up.
printf "--- coreml-smart-crop daemon started at %s ---\n" "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" > "$LOG_FILE"

# Always set absolute paths so the daemon finds the model (ignore stale env like ./models/yolo.mlpackage).
# Prefer .mlmodelc if present (Swift compiles .mlpackage to .mlmodelc on first use).
if [ -d "$ROACH_ROOT/data/models/yolo.mlmodelc" ]; then
    export YOLO_MODEL_PATH="$ROACH_ROOT/data/models/yolo.mlmodelc"
else
    export YOLO_MODEL_PATH="$ROACH_ROOT/data/models/yolo.mlpackage"
fi
export WATCH_ROOT="${WATCH_ROOT:-$ROACH_ROOT/data/streams/unifi/protect/smart}"
export COREML_OUTPUT_DIR="${COREML_OUTPUT_DIR:-$ROACH_ROOT/data/streams/coreml}"

echo "🚀 Starting coreml-smart-crop daemon..."
cd "$ROACH_ROOT" && nohup env YOLO_MODEL_PATH="$YOLO_MODEL_PATH" WATCH_ROOT="$WATCH_ROOT" COREML_OUTPUT_DIR="$COREML_OUTPUT_DIR" "$BIN" >> "$LOG_FILE" 2>&1 &
DPID=$!
echo "$DPID" > "$PID_FILE"

echo "✅ coreml-smart-crop started (PID $DPID)"
echo "   Logs: $LOG_FILE"
echo "   Stop: ./scripts/coreml-smart-crop/run/stop.sh"
