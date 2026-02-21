#!/bin/bash
# Run detect-person in foreground for launchd. Do not fork — launchd tracks this process.
# Usage: run from project root, or set ROACH_ROOT to project root.
#   ROACH_ROOT=/path/to/roach ./scripts/detect-person/run/run-daemon.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="${ROACH_ROOT:-$(cd "$SCRIPT_DIR/../../.." && pwd)}"
cd "$ROACH_ROOT"

# Load .env so KAFKA_BROKER etc. are set (e.g. when run by launchd)
if [ -f "$ROACH_ROOT/.env" ]; then
    set -a
    # shellcheck source=/dev/null
    source "$ROACH_ROOT/.env"
    set +a
fi

# Build if needed
if [ ! -d services/detect-person/.build ]; then
    "$ROACH_ROOT/scripts/detect-person/build/build.sh"
fi

# Prefer release binary if present
BIN="$ROACH_ROOT/services/detect-person/.build/release/DetectPerson"
if [ ! -x "$BIN" ]; then
    BIN="$ROACH_ROOT/services/detect-person/.build/debug/DetectPerson"
fi
[ -x "$BIN" ] || { echo "No built binary; run ./scripts/detect-person/build/build.sh" >&2; exit 1; }

exec "$BIN" detect
