#!/bin/bash
# Run person detection in foreground (interactive)
# Usage: ./scripts/detect-person/run/detect.sh [options]
# Environment: KAFKA_BROKER, WATCH_DIR, CONFIDENCE_THRESHOLD, etc.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

echo "🔍 Starting detect-person (foreground)..."
echo "   Press Ctrl+C to stop"
echo ""

# Ensure built
if [ ! -d services/detect-person/.build ]; then
    echo "📦 Building first..."
    ./scripts/detect-person/build/build.sh
fi

cd services/detect-person
swift run DetectPerson detect "$@"
