#!/bin/bash
# Ensure YOLO Core ML model exists at ./models/yolo.mlpackage for coreml-smart-crop.
# Option 1: Export via Ultralytics (recommended). Option 2: Copy a pre-exported .mlpackage to models/yolo.mlpackage.
# Usage: ./scripts/models/download-yolo.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROACH_ROOT"

MODELS_DIR="models"
TARGET="$MODELS_DIR/yolo.mlpackage"

mkdir -p "$MODELS_DIR"

if [ -d "$TARGET" ] || [ -f "${TARGET}.zip" ]; then
    echo "✅ YOLO model already present at $TARGET"
    exit 0
fi

# Try exporting with Ultralytics (yolo11n or yolov8n)
if command -v python3 &>/dev/null && python3 -c "import ultralytics" 2>/dev/null; then
    echo "🔨 Exporting YOLO to Core ML (Ultralytics)..."
    TMP_DIR="$(mktemp -d)"
    trap "rm -rf '$TMP_DIR'" EXIT
    ( cd "$TMP_DIR" && python3 -c "
from ultralytics import YOLO
m = YOLO('yolo11n.pt')
m.export(format='coreml', nms=True)
" )
    PKG=$(find "$TMP_DIR" -maxdepth 1 -type d -name '*.mlpackage' 2>/dev/null | head -1)
    if [ -n "$PKG" ] && [ -d "$PKG" ]; then
        cp -R "$PKG" "$ROACH_ROOT/$TARGET"
        echo "✅ Exported YOLO model to $TARGET"
        exit 0
    fi
    ( cd "$TMP_DIR" && python3 -c "
from ultralytics import YOLO
m = YOLO('yolov8n.pt')
m.export(format='coreml', nms=True)
" )
    PKG=$(find "$TMP_DIR" -maxdepth 1 -type d -name '*.mlpackage' 2>/dev/null | head -1)
    if [ -n "$PKG" ] && [ -d "$PKG" ]; then
        cp -R "$PKG" "$ROACH_ROOT/$TARGET"
        echo "✅ Exported YOLO model to $TARGET"
        exit 0
    fi
fi

echo "⚠️  YOLO model not found at $TARGET"
echo ""
echo "Create it with one of:"
echo "  1. Export via Ultralytics (from project root):"
echo "     pip install ultralytics"
echo "     yolo export model=yolo11n.pt format=coreml nms=True"
echo "     mv yolo11n.mlpackage $TARGET"
echo "  2. Or use yolov8n.pt: yolo export model=yolov8n.pt format=coreml nms=True"
echo ""
echo "coreml-smart-crop will fail at startup if the model is missing."
exit 1
