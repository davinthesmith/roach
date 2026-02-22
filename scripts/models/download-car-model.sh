#!/bin/bash
# Ensure Car Recognition Core ML model exists at ./data/models/CarRecognition.mlmodel for coreml-vehicle-detect.
# Downloads the pre-built .mlmodel from Core-ML-Car-Recognition repo (CarRecognition/Resources).
# Usage: ./scripts/models/download-car-model.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROACH_ROOT"

MODELS_DIR="data/models"
TARGET="$MODELS_DIR/CarRecognition.mlmodel"
SOURCE_URL="https://raw.githubusercontent.com/likedan/Core-ML-Car-Recognition/master/CarRecognition/Resources/CarRecognition.mlmodel"

mkdir -p "$MODELS_DIR"

if [ -f "$TARGET" ]; then
    echo "✅ Car model already present at $TARGET"
    exit 0
fi

echo "📥 Downloading CarRecognition.mlmodel from Core-ML-Car-Recognition..."
if command -v curl &>/dev/null; then
    curl -sSL -o "$TARGET" "$SOURCE_URL"
elif command -v wget &>/dev/null; then
    wget -q -O "$TARGET" "$SOURCE_URL"
else
    echo "❌ Need curl or wget to download the model" >&2
    exit 1
fi

if [ ! -s "$TARGET" ]; then
    echo "❌ Download failed or empty file at $TARGET" >&2
    rm -f "$TARGET"
    exit 1
fi

echo "✅ Car model saved to $TARGET"

# Compile to .mlmodelc so Core ML can load it at runtime
COMPILED="$MODELS_DIR/CarRecognition.mlmodelc"
if [ ! -d "$COMPILED" ] || [ "$TARGET" -nt "$COMPILED" ]; then
    echo "🔨 Compiling to $COMPILED..."
    xcrun coremlcompiler compile "$TARGET" "$MODELS_DIR"
    echo "✅ Compiled to $COMPILED"
else
    echo "✅ Compiled model already present at $COMPILED"
fi
