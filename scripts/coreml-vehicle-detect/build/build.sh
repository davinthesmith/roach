#!/bin/bash
# Build the coreml-vehicle-detect Swift package
# Usage: ./scripts/coreml-vehicle-detect/build/build.sh [release]
#
# Uses macOS 15 target so swift-kafka-client (ExecutorJob/UnownedTaskExecutor) compiles.

set -e

MODE="debug"
FLAGS=""
if [ "$1" = "release" ]; then
    MODE="release"
    FLAGS="-c release"
fi

# Force macOS 15 so swift-kafka-client's DispatchQueueTaskExecutor compiles
ARCH=$(uname -m)
TRIPLE="${ARCH}-apple-macosx15.0"
EXTRA="-Xswiftc -target -Xswiftc ${TRIPLE}"

echo "🔨 Building coreml-vehicle-detect ($MODE)..."
cd "$(dirname "$0")/../../../services/coreml-vehicle-detect"
swift build $FLAGS $EXTRA
echo "✅ Build complete!"
