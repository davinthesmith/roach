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

# Always set absolute paths so the process finds the model (ignore stale env like ./models/yolo.mlpackage).
# Prefer .mlmodelc if present (Swift compiles .mlpackage to .mlmodelc on first use).
if [ -d "$ROACH_ROOT/data/models/yolo.mlmodelc" ]; then
    export YOLO_MODEL_PATH="$ROACH_ROOT/data/models/yolo.mlmodelc"
else
    export YOLO_MODEL_PATH="$ROACH_ROOT/data/models/yolo.mlpackage"
fi
export WATCH_ROOT="${WATCH_ROOT:-$ROACH_ROOT/data/streams/unifi/protect/smart}"
export COREML_OUTPUT_DIR="${COREML_OUTPUT_DIR:-$ROACH_ROOT/data/streams/coreml}"

exec "$BIN"
