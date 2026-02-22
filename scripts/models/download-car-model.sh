#!/bin/bash
# Ensure Car Recognition Core ML model exists at ./models/CarRecognition.mlmodel for coreml-vehicle-detect.
# The Core-ML-Car-Recognition repo provides a Caffe model and convert.py; you must run the conversion
# to produce CarRecognition.mlmodel, or place a pre-converted .mlmodel at the path below.
# Usage: ./scripts/models/download-car-model.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROACH_ROOT"

MODELS_DIR="models"
TARGET="$MODELS_DIR/CarRecognition.mlmodel"

mkdir -p "$MODELS_DIR"

if [ -f "$TARGET" ]; then
    echo "✅ Car model already present at $TARGET"
    exit 0
fi

# No direct .mlmodel in the repo (only Caffe + convert.py). Print instructions.
echo "⚠️  Car Recognition model not found at $TARGET"
echo ""
echo "The Core-ML-Car-Recognition repo provides a Caffe model and Python script to convert to Core ML."
echo "To create the model:"
echo "  1. git clone https://github.com/likedan/Core-ML-Car-Recognition.git /tmp/Core-ML-Car-Recognition"
echo "  2. cd /tmp/Core-ML-Car-Recognition/Convert"
echo "  3. pip install coremltools"
echo "  4. python convert.py   # produces Core ML model"
echo "  5. cp <output>.mlmodel $ROACH_ROOT/$TARGET"
echo ""
echo "Or place an existing CarRecognition.mlmodel (CompCars-based) at $TARGET"
echo ""
echo "coreml-vehicle-detect will fail at startup if the model is missing."
exit 1
