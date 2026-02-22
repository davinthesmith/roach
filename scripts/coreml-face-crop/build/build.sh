#!/bin/bash
# Build the coreml-face-crop Swift package
# Usage: ./scripts/coreml-face-crop/build/build.sh [release]

set -e

MODE="debug"
FLAGS=""
if [ "$1" = "release" ]; then
    MODE="release"
    FLAGS="-c release"
fi

echo "🔨 Building coreml-face-crop ($MODE)..."
cd "$(dirname "$0")/../../../services/coreml-face-crop"
swift build $FLAGS
echo "✅ Build complete!"
