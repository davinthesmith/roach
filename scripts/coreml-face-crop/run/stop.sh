#!/bin/bash
# Stop the coreml-face-crop background daemon
# Usage: ./scripts/coreml-face-crop/run/stop.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

PID_FILE="data/logs/coreml-face-crop.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "⚠️  coreml-face-crop is not running (no PID file)"
    exit 0
fi

PID=$(cat "$PID_FILE")

if ! kill -0 "$PID" 2>/dev/null; then
    echo "⚠️  coreml-face-crop is not running (stale PID $PID)"
    rm -f "$PID_FILE"
    exit 0
fi

echo "🛑 Stopping coreml-face-crop (PID $PID)..."
kill "$PID"

# Wait up to 10 seconds for graceful shutdown
for i in $(seq 1 10); do
    if ! kill -0 "$PID" 2>/dev/null; then
        rm -f "$PID_FILE"
        echo "✅ coreml-face-crop stopped"
        exit 0
    fi
    sleep 1
done

echo "⚠️  Forcing stop..."
kill -9 "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
echo "✅ coreml-face-crop stopped (forced)"
