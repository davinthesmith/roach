#!/usr/bin/env bash
#
# Send thermostat commands to the homeassistant.command Kafka topic.
#
# Usage:
#   ./scripts/homeassistant/send-command.sh set_temperature 72
#   ./scripts/homeassistant/send-command.sh set_temperature 72 --entity climate.sneaux
#   ./scripts/homeassistant/send-command.sh set_hvac_mode heat
#   ./scripts/homeassistant/send-command.sh set_hvac_mode heat --entity climate.sneaux
#   ./scripts/homeassistant/send-command.sh set_preset_mode away
#   ./scripts/homeassistant/send-command.sh set_fan_mode auto
#   ./scripts/homeassistant/send-command.sh turn_off
#   ./scripts/homeassistant/send-command.sh turn_on
#
# Environment:
#   HA_THERMOSTAT_ENTITY  Default entity ID (default: climate.sneaux)
#   KAFKA_CONTAINER       Docker container name (default: roach-kafka)
#   KAFKA_TOPIC           Kafka topic (default: homeassistant.command)

set -euo pipefail

ENTITY="${HA_THERMOSTAT_ENTITY:-climate.sneaux}"
CONTAINER="${KAFKA_CONTAINER:-roach-kafka}"
TOPIC="${KAFKA_TOPIC:-homeassistant.command}"

usage() {
    cat <<EOF
Usage: $(basename "$0") <service> [value] [--entity <entity_id>]

Climate services:
  set_temperature <temp>     Set target temperature (e.g. 72)
  set_hvac_mode <mode>       Set HVAC mode (off, heat, cool, heat_cool, auto)
  set_preset_mode <preset>   Set preset mode (away, home, sleep)
  set_fan_mode <mode>        Set fan mode (auto, on)
  turn_on                    Turn on the thermostat
  turn_off                   Turn off the thermostat

Options:
  --entity <entity_id>       Target entity (default: \$HA_THERMOSTAT_ENTITY or climate.sneaux)

Examples:
  $(basename "$0") set_temperature 72
  $(basename "$0") set_hvac_mode heat --entity climate.sneaux
  $(basename "$0") set_preset_mode away
  $(basename "$0") turn_off
EOF
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

SERVICE="$1"
shift

VALUE=""
# Parse remaining arguments
while [ $# -gt 0 ]; do
    case "$1" in
        --entity)
            ENTITY="$2"
            shift 2
            ;;
        *)
            VALUE="$1"
            shift
            ;;
    esac
done

# Build the JSON command
case "$SERVICE" in
    set_temperature)
        if [ -z "$VALUE" ]; then
            echo "Error: set_temperature requires a temperature value"
            exit 1
        fi
        JSON=$(printf '{"domain":"climate","service":"set_temperature","entity_id":"%s","data":{"temperature":%s}}' "$ENTITY" "$VALUE")
        ;;
    set_hvac_mode)
        if [ -z "$VALUE" ]; then
            echo "Error: set_hvac_mode requires a mode value (off, heat, cool, heat_cool, auto)"
            exit 1
        fi
        JSON=$(printf '{"domain":"climate","service":"set_hvac_mode","entity_id":"%s","data":{"hvac_mode":"%s"}}' "$ENTITY" "$VALUE")
        ;;
    set_preset_mode)
        if [ -z "$VALUE" ]; then
            echo "Error: set_preset_mode requires a preset value (away, home, sleep)"
            exit 1
        fi
        JSON=$(printf '{"domain":"climate","service":"set_preset_mode","entity_id":"%s","data":{"preset_mode":"%s"}}' "$ENTITY" "$VALUE")
        ;;
    set_fan_mode)
        if [ -z "$VALUE" ]; then
            echo "Error: set_fan_mode requires a mode value (auto, on)"
            exit 1
        fi
        JSON=$(printf '{"domain":"climate","service":"set_fan_mode","entity_id":"%s","data":{"fan_mode":"%s"}}' "$ENTITY" "$VALUE")
        ;;
    turn_on)
        JSON=$(printf '{"domain":"climate","service":"turn_on","entity_id":"%s","data":{}}' "$ENTITY")
        ;;
    turn_off)
        JSON=$(printf '{"domain":"climate","service":"turn_off","entity_id":"%s","data":{}}' "$ENTITY")
        ;;
    *)
        echo "Error: unknown service '$SERVICE'"
        usage
        ;;
esac

echo "Sending command to $TOPIC:"
echo "  $JSON"
echo ""

echo "$JSON" | docker exec -i "$CONTAINER" kafka-console-producer \
    --broker-list localhost:29092 \
    --topic "$TOPIC"

echo "Command sent successfully."
