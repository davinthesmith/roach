#!/bin/bash
# Run coreml-face-crop in foreground (interactive)
# Usage: ./scripts/coreml-face-crop/run/detect.sh
# Environment: WATCH_DIR, FACES_DIR, DEBOUNCE_SECONDS, LOG_LEVEL

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

echo "🔍 Starting coreml-face-crop (foreground)..."
echo "   Press Ctrl+C to stop"
echo ""

# Ensure built
if [ ! -d services/coreml-face-crop/.build ]; then
    echo "📦 Building first..."
    ./scripts/coreml-face-crop/build/build.sh
fi

BIN="$ROACH_ROOT/services/coreml-face-crop/.build/release/CoreMLFaceCrop"
[ -x "$BIN" ] || BIN="$ROACH_ROOT/services/coreml-face-crop/.build/debug/CoreMLFaceCrop"
[ -x "$BIN" ] || { echo "No built binary; run ./scripts/coreml-face-crop/build/build.sh" >&2; exit 1; }

exec "$BIN"
