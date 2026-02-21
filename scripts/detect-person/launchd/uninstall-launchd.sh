#!/bin/bash
# Remove detect-person LaunchAgent (stops auto-start at login).
# Usage: ./scripts/detect-person/launchd/uninstall-launchd.sh

set -e

PLIST_NAME="com.roach.detect-person.plist"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"
PLIST_PATH="$LAUNCH_AGENTS/$PLIST_NAME"

if [ -f "$PLIST_PATH" ]; then
    launchctl list "$PLIST_NAME" &>/dev/null && launchctl unload "$PLIST_PATH"
    rm -f "$PLIST_PATH"
    echo "Uninstalled: $PLIST_PATH"
else
    echo "Not installed (no $PLIST_PATH)"
fi
