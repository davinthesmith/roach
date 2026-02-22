#!/bin/bash
# Check coreml-smart-crop status
# Usage: ./scripts/coreml-smart-crop/run/status.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

echo "🔍 coreml-smart-crop Status"
echo "==========================="
echo ""

PID_FILE="data/logs/coreml-smart-crop.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        UPTIME=$(ps -o etime= -p "$PID" | xargs)
        echo "  ✅ Running (PID $PID, uptime: $UPTIME)"
    else
        echo "  ❌ Not running (stale PID file)"
    fi
else
    echo "  ❌ Not running"
fi
echo ""

WATCH_ROOT="${WATCH_ROOT:-./data/streams/unifi/protect/smart}"
if [ -d "$WATCH_ROOT" ]; then
    echo "👁️  Watch root: $WATCH_ROOT"
else
    echo "👁️  Watch root: not found ($WATCH_ROOT)"
fi
echo ""

OUTPUT_BASE="${COREML_OUTPUT_DIR:-./data/streams/coreml}"
for type in person package animal vehicle; do
    OUT="$OUTPUT_BASE/$type"
    if [ -d "$OUT" ]; then
        CROP_COUNT=$(find "$OUT" -maxdepth 1 -name "*.jpg" 2>/dev/null | wc -l | xargs)
        echo "📂 $type: $CROP_COUNT images ($OUT)"
    fi
done
echo ""

LOG_FILE="data/logs/coreml-smart-crop.log"
if [ -f "$LOG_FILE" ]; then
    LOG_SIZE=$(du -sh "$LOG_FILE" | cut -f1 | xargs)
    echo "📋 Log file: $LOG_FILE ($LOG_SIZE)"
else
    echo "📋 Log file: none yet"
fi
