#!/bin/bash
# Stop the coreml-face-crop background daemon
# Usage: ./scripts/coreml-face-crop/run/stop.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

PID_FILE="data/logs/coreml-face-crop.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "🛑 Stopping coreml-face-crop (PID $PID)..."
        kill "$PID"
        for i in $(seq 1 10); do
            if ! kill -0 "$PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        if kill -0 "$PID" 2>/dev/null; then
            echo "⚠️  Forcing stop..."
            kill -9 "$PID" 2>/dev/null || true
        fi
    else
        echo "⚠️  coreml-face-crop not running (stale PID $PID)"
    fi
    rm -f "$PID_FILE"
else
    echo "⚠️  coreml-face-crop has no PID file"
fi

# Cleanup: kill any other CoreMLFaceCrop processes from this project (e.g. old runs, wrong CWD)
PATTERN="coreml-face-crop/.build.*CoreMLFaceCrop"
EXTRA=$(pgrep -f "$PATTERN" 2>/dev/null || true)
if [ -n "$EXTRA" ]; then
    echo "🧹 Stopping $([ $(echo "$EXTRA" | wc -w) -gt 1 ] && echo 'other ' || echo '')coreml-face-crop process(es): $EXTRA"
    echo "$EXTRA" | xargs kill 2>/dev/null || true
    sleep 2
    echo "$EXTRA" | xargs kill -9 2>/dev/null || true
fi
echo "✅ coreml-face-crop stopped"
