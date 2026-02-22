#!/bin/bash
# Check coreml-face-crop status
# Usage: ./scripts/coreml-face-crop/run/status.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

echo "🔍 coreml-face-crop Status"
echo "=========================="
echo ""

PID_FILE="data/logs/coreml-face-crop.pid"

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

WATCH_DIR="${WATCH_DIR:-./data/streams/coreml/person}"
if [ -d "$WATCH_DIR" ]; then
    IMG_COUNT=$(find "$WATCH_DIR" -maxdepth 1 -name "*.jpg" 2>/dev/null | wc -l | xargs)
    echo "👁️  Watch directory: $IMG_COUNT images ($WATCH_DIR)"
else
    echo "👁️  Watch directory: not found ($WATCH_DIR)"
fi
echo ""

FACES_DIR="${FACES_DIR:-./data/streams/coreml/faces}"
if [ -d "$FACES_DIR" ]; then
    FACE_COUNT=$(find "$FACES_DIR" -maxdepth 1 -name "*.jpg" 2>/dev/null | wc -l | xargs)
    echo "📂 Output (faces): $FACE_COUNT images ($FACES_DIR)"
else
    echo "📂 Output: not found ($FACES_DIR)"
fi
echo ""

LOG_FILE="data/logs/coreml-face-crop.log"
if [ -f "$LOG_FILE" ]; then
    LOG_SIZE=$(du -sh "$LOG_FILE" | cut -f1 | xargs)
    echo "📋 Log file: $LOG_FILE ($LOG_SIZE)"
else
    echo "📋 Log file: none yet"
fi
