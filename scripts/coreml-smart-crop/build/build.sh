#!/bin/bash
# Build the coreml-smart-crop Swift package
# Usage: ./scripts/coreml-smart-crop/build/build.sh [release]

set -e

MODE="debug"
FLAGS=""
if [ "$1" = "release" ]; then
    MODE="release"
    FLAGS="-c release"
fi

echo "🔨 Building coreml-smart-crop ($MODE)..."
cd "$(dirname "$0")/../../../services/coreml-smart-crop"
swift build $FLAGS
echo "✅ Build complete!"
