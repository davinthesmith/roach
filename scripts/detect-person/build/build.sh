#!/bin/bash
# Build the detect-person Swift package
# Usage: ./scripts/detect-person/build/build.sh [release]
#   release - Build in release mode (optimized, slower build)
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

echo "🔨 Building detect-person ($MODE)..."
cd "$(dirname "$0")/../../../services/detect-person"
swift build $FLAGS $EXTRA
echo "✅ Build complete!"
