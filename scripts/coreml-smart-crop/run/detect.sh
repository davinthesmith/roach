#!/bin/bash
# Run coreml-smart-crop in foreground (interactive)
# Usage: ./scripts/coreml-smart-crop/run/detect.sh
# Environment: WATCH_ROOT, COREML_OUTPUT_DIR, YOLO_MODEL_PATH, DEBOUNCE_SECONDS, LOG_LEVEL

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

echo "🔍 Starting coreml-smart-crop (foreground)..."
echo "   Press Ctrl+C to stop"
echo ""

# Ensure built
if [ ! -d services/coreml-smart-crop/.build ]; then
    echo "📦 Building first..."
    ./scripts/coreml-smart-crop/build/build.sh
fi

BIN="$ROACH_ROOT/services/coreml-smart-crop/.build/release/CoreMLSmartCrop"
[ -x "$BIN" ] || BIN="$ROACH_ROOT/services/coreml-smart-crop/.build/debug/CoreMLSmartCrop"
[ -x "$BIN" ] || { echo "No built binary; run ./scripts/coreml-smart-crop/build/build.sh" >&2; exit 1; }

exec "$BIN"
