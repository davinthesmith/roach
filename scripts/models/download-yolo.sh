#!/bin/bash
# Ensure YOLO Core ML model exists at ./data/models/yolo.mlpackage for coreml-smart-crop.
# Downloads the pre-built yolov8n.mlpackage from Hugging Face (TheCluster/YOLOv8-CoreML).
# Requires: git, git-lfs (brew install git-lfs).
# Usage: ./scripts/models/download-yolo.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROACH_ROOT"

MODELS_DIR="data/models"
TARGET="$MODELS_DIR/yolo.mlpackage"
REPO_URL="https://huggingface.co/TheCluster/YOLOv8-CoreML"
PATH_IN_REPO="yolov8n.mlpackage"

mkdir -p "$MODELS_DIR"

if [ -d "$TARGET" ]; then
    echo "✅ YOLO model already present at $TARGET"
    exit 0
fi

if ! command -v git &>/dev/null; then
    echo "❌ git is required. Install with: brew install git" >&2
    exit 1
fi

# Git LFS is required to fetch actual model files (not pointers)
if ! command -v git-lfs &>/dev/null; then
    echo "❌ git-lfs is required. Install with: brew install git-lfs && git lfs install" >&2
    exit 1
fi

TMP=$(mktemp -d)
trap "rm -rf '$TMP'" EXIT

echo "📥 Downloading YOLO Core ML model (yolov8n) from Hugging Face..."

# Shallow clone with sparse checkout so we only get the nano model folder
git clone --depth 1 --filter=blob:none --sparse "$REPO_URL" "$TMP/repo"
cd "$TMP/repo"
git sparse-checkout set "$PATH_IN_REPO"
git checkout
git lfs pull --include="$PATH_IN_REPO/*"

if [ ! -d "$TMP/repo/$PATH_IN_REPO" ]; then
    echo "❌ Download failed: $PATH_IN_REPO not found" >&2
    exit 1
fi

mv "$TMP/repo/$PATH_IN_REPO" "$ROACH_ROOT/$TARGET"
echo "✅ YOLO model saved to $TARGET"
