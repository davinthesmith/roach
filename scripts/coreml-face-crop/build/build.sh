#!/bin/bash
# Build the coreml-face-crop Swift package
# Usage: ./scripts/coreml-face-crop/build/build.sh [release|clean]
#   release = build in release mode
#   clean   = remove .build then build (force full rebuild)

set -e

MODE="debug"
FLAGS=""
if [ "$1" = "release" ]; then
    MODE="release"
    FLAGS="-c release"
elif [ "$1" = "clean" ]; then
    MODE="debug"
    CLEAN=1
fi

cd "$(dirname "$0")/../../../services/coreml-face-crop"
if [ -n "${CLEAN-}" ]; then
    echo "🧹 Cleaning .build..."
    rm -rf .build
fi
echo "🔨 Building coreml-face-crop ($MODE)..."
swift build $FLAGS
echo "✅ Build complete!"
