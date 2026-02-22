#!/bin/bash
# View coreml-vehicle-detect logs
# Usage: ./scripts/coreml-vehicle-detect/run/logs.sh [lines]
#   lines - Number of initial lines to show (default: 50)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

LOG_FILE="data/logs/coreml-vehicle-detect.log"
LINES=${1:-50}

if [ ! -f "$LOG_FILE" ]; then
    echo "📋 No log file yet. Start coreml-vehicle-detect first:"
    echo "   ./scripts/coreml-vehicle-detect/run/start.sh"
    exit 0
fi

echo "📋 coreml-vehicle-detect logs ($LOG_FILE)"
echo "   Press Ctrl+C to stop following"
echo ""
tail -n "$LINES" -f "$LOG_FILE"
