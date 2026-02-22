#!/bin/bash
# Build the coreml-vehicle-detect Swift package
# Usage: ./scripts/coreml-vehicle-detect/build/build.sh [release|clean]
#   release = build in release mode
#   clean   = remove .build then build (force full rebuild)
#
# Uses macOS 15 target so swift-kafka-client (ExecutorJob/UnownedTaskExecutor) compiles.

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

# Force macOS 15 so swift-kafka-client's DispatchQueueTaskExecutor compiles
ARCH=$(uname -m)
TRIPLE="${ARCH}-apple-macosx15.0"
EXTRA="-Xswiftc -target -Xswiftc ${TRIPLE}"

cd "$(dirname "$0")/../../../services/coreml-vehicle-detect"
if [ -n "${CLEAN-}" ]; then
    echo "🧹 Cleaning .build..."
    rm -rf .build
fi
echo "🔨 Building coreml-vehicle-detect ($MODE)..."
swift build $FLAGS $EXTRA
echo "✅ Build complete!"
