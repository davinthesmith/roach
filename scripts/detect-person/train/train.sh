#!/bin/bash
# Train the person classification model using CreateML
# Usage: ./scripts/detect-person/train/train.sh [options]
#   --train-dir <path>   Training data directory (default: ./data/train)
#   --model-dir <path>   Model output directory (default: ./data/models/detect-person)
#   --max-iterations <n> Max training iterations

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$ROACH_ROOT"

echo "🧠 Starting detect-person training..."

# Ensure built
if [ ! -d services/detect-person/.build ]; then
    echo "📦 Building first..."
    ./scripts/detect-person/build/build.sh
fi

# Forward all arguments to the train subcommand
cd services/detect-person
swift run DetectPerson train "$@"
