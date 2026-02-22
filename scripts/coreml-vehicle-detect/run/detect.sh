#!/bin/bash
# Run coreml-vehicle-detect in foreground (interactive)
# Usage: ./scripts/coreml-vehicle-detect/run/detect.sh
# Environment: WATCH_DIR, CAR_MODEL_PATH, KAFKA_BROKER, KAFKA_TOPIC, DEBOUNCE_SECONDS, LOG_LEVEL

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

echo "🔍 Starting coreml-vehicle-detect (foreground)..."
echo "   Press Ctrl+C to stop"
echo ""

# Ensure built
if [ ! -d services/coreml-vehicle-detect/.build ]; then
    echo "📦 Building first..."
    ./scripts/coreml-vehicle-detect/build/build.sh
fi

BIN="$ROACH_ROOT/services/coreml-vehicle-detect/.build/release/CoreMLVehicleDetect"
[ -x "$BIN" ] || BIN="$ROACH_ROOT/services/coreml-vehicle-detect/.build/debug/CoreMLVehicleDetect"
[ -x "$BIN" ] || { echo "No built binary; run ./scripts/coreml-vehicle-detect/build/build.sh" >&2; exit 1; }

exec "$BIN"
