#!/bin/bash
# Install detect-person as a LaunchAgent so it starts at login and restarts after reboot.
# Usage: ./scripts/detect-person/launchd/install-launchd.sh
# Uninstall: ./scripts/detect-person/launchd/uninstall-launchd.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROACH_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PLIST_NAME="com.roach.detect-person.plist"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
TEMPLATE="$SCRIPT_DIR/com.roach.detect-person.plist.template"
PLIST_DEST="$LAUNCH_AGENTS/$PLIST_NAME"

if [ ! -f "$TEMPLATE" ]; then
    echo "Template not found: $TEMPLATE"
    exit 1
fi

mkdir -p "$ROACH_ROOT/data/logs"
mkdir -p "$LAUNCH_AGENTS"

sed "s|__ROACH_PROJECT_ROOT__|$ROACH_ROOT|g" "$TEMPLATE" > "$PLIST_DEST"
echo "Installed: $PLIST_DEST"

# Unload if already loaded (e.g. reinstall)
launchctl list "$PLIST_NAME" &>/dev/null && launchctl unload "$PLIST_DEST"
launchctl load "$PLIST_DEST"
echo "Loaded. detect-person will start at login and restart if it exits."
echo "Logs: $ROACH_ROOT/data/logs/detect-person.log"
