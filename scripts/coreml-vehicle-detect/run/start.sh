#!/bin/bash
# Start coreml-vehicle-detect as a background daemon
# Usage: ./scripts/coreml-vehicle-detect/run/start.sh
# Logs: data/logs/coreml-vehicle-detect.log
# PID:  data/logs/coreml-vehicle-detect.pid

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

LOG_DIR="data/logs"
LOG_FILE="$LOG_DIR/coreml-vehicle-detect.log"
PID_FILE="$LOG_DIR/coreml-vehicle-detect.pid"

# Check if already running
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "⚠️  coreml-vehicle-detect is already running (PID $PID)"
        echo "   Stop first: ./scripts/coreml-vehicle-detect/run/stop.sh"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

# Ensure built
if [ ! -d services/coreml-vehicle-detect/.build ]; then
    echo "📦 Building first..."
    ./scripts/coreml-vehicle-detect/build/build.sh
fi

BIN="$ROACH_ROOT/services/coreml-vehicle-detect/.build/release/CoreMLVehicleDetect"
[ -x "$BIN" ] || BIN="$ROACH_ROOT/services/coreml-vehicle-detect/.build/debug/CoreMLVehicleDetect"
[ -x "$BIN" ] || { echo "No built binary; run ./scripts/coreml-vehicle-detect/build/build.sh" >&2; exit 1; }

mkdir -p "$LOG_DIR"

# Use absolute paths so the daemon works regardless of cwd. Prefer .mlmodelc if present.
if [ -d "$ROACH_ROOT/data/models/CarRecognition.mlmodelc" ]; then
    export CAR_MODEL_PATH="${CAR_MODEL_PATH:-$ROACH_ROOT/data/models/CarRecognition.mlmodelc}"
else
    export CAR_MODEL_PATH="${CAR_MODEL_PATH:-$ROACH_ROOT/data/models/CarRecognition.mlmodel}"
fi
export WATCH_DIR="${WATCH_DIR:-$ROACH_ROOT/data/streams/coreml/vehicle}"

echo "🚀 Starting coreml-vehicle-detect daemon..."
cd "$ROACH_ROOT" && nohup env CAR_MODEL_PATH="$CAR_MODEL_PATH" WATCH_DIR="$WATCH_DIR" "$BIN" >> "$LOG_FILE" 2>&1 &
DPID=$!
echo "$DPID" > "$PID_FILE"

echo "✅ coreml-vehicle-detect started (PID $DPID)"
echo "   Logs: $LOG_FILE"
echo "   Stop: ./scripts/coreml-vehicle-detect/run/stop.sh"
