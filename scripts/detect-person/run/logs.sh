#!/bin/bash
# View detect-person logs
# Usage: ./scripts/detect-person/run/logs.sh [lines]
#   lines - Number of initial lines to show (default: 50)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

LOG_FILE="data/logs/detect-person.log"
LINES=${1:-50}

if [ ! -f "$LOG_FILE" ]; then
    echo "📋 No log file yet. Start detect-person first:"
    echo "   ./scripts/detect-person/run/start.sh"
    exit 0
fi

echo "📋 detect-person logs ($LOG_FILE)"
echo "   Press Ctrl+C to stop following"
echo ""
tail -n "$LINES" -f "$LOG_FILE"
