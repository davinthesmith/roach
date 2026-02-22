#!/bin/bash
# Stop the coreml-smart-crop background daemon
# Usage: ./scripts/coreml-smart-crop/run/stop.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

PID_FILE="data/logs/coreml-smart-crop.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "🛑 Stopping coreml-smart-crop (PID $PID)..."
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
        echo "⚠️  coreml-smart-crop not running (stale PID $PID)"
    fi
    rm -f "$PID_FILE"
else
    echo "⚠️  coreml-smart-crop has no PID file"
fi

# Cleanup: kill any other CoreMLSmartCrop processes from this project (e.g. old runs, wrong CWD)
PATTERN="coreml-smart-crop/.build.*CoreMLSmartCrop"
EXTRA=$(pgrep -f "$PATTERN" 2>/dev/null || true)
if [ -n "$EXTRA" ]; then
    echo "🧹 Stopping $([ $(echo "$EXTRA" | wc -w) -gt 1 ] && echo 'other ' || echo '')coreml-smart-crop process(es): $EXTRA"
    echo "$EXTRA" | xargs kill 2>/dev/null || true
    sleep 2
    echo "$EXTRA" | xargs kill -9 2>/dev/null || true
fi
echo "✅ coreml-smart-crop stopped"
