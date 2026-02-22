#!/bin/bash
# View coreml-face-crop logs
# Usage: ./scripts/coreml-face-crop/run/logs.sh [lines]
#   lines - Number of initial lines to show (default: 50)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

LOG_FILE="data/logs/coreml-face-crop.log"
LINES=${1:-50}

if [ ! -f "$LOG_FILE" ]; then
    echo "📋 No log file yet. Start coreml-face-crop first:"
    echo "   ./scripts/coreml-face-crop/run/start.sh"
    exit 0
fi

echo "📋 coreml-face-crop logs ($LOG_FILE)"
echo "   Press Ctrl+C to stop following"
echo ""
tail -n "$LINES" -f "$LOG_FILE"
