# Kafka Topics

> **Overview**: [CLAUDE.md](../CLAUDE.md). **Standards**: [kafka-standards.md](kafka-standards.md). This doc: topic list, message format, key/header conventions.

## Naming

Format: `namespace.category[.subcategory]`. Topics auto-created on first publish. Retention: infinite (broker). Partitions: 1 (single broker).

## Weather (weatherlink-kafka)

**Data** (published every `FETCH_INTERVAL`): `weather.iss`, `weather.barometer`, `weather.indoor`, `weather.health`, `weather.other`.  
**Metadata** (published on `METADATA_FETCH_INTERVAL`, key-deduped): `weather.metadata.sensors`, `weather.metadata.catalog`, `weather.metadata.station`.

| Topic | Description | Key |
|-------|-------------|-----|
| weather.iss | Outdoor (ISS): temp, hum, wind_*, rain_*, solar_rad, uv_index, dew_point, heat_index, wind_chill, thw/thsw_index | lsid:timestamp |
| weather.barometer | bar_sea_level, bar_absolute, bar_trend | lsid:timestamp |
| weather.indoor | temp_in, hum_in, dew_point_in, heat_index_in | lsid:timestamp |
| weather.health | battery_voltage, uptime, wifi_rssi, firmware_version, free_mem, etc. | lsid:timestamp |
| weather.other | Fallback for unknown category | lsid:timestamp |
| weather.metadata.sensors | Sensor config (LSID, etc.) | lsid:weekStart |
| weather.metadata.catalog | Field schemas per sensor type (one message per type) | sensor_type:max_data_structure_type |
| weather.metadata.station | Station name, location, timezone | station_id:weekStart |

## UniFi Protect (unifi-kafka)

Key: `{camera_name}:{timestamp}` (camera name sanitized: lowercase, spaces→underscores). Timestamp from event `start` (ms→s).

| Topic | Detection types | Consumer |
|-------|-----------------|----------|
| unifi.protect.smart | person, vehicle, animal, package | unifi-smart-archive (image archiving) |
| unifi.protect.audio | babyCry, coAlarm, smoke, speak | — |
| unifi.protect.motion | motion | — |

Multiple distinct smart events per camera (overlapping or adjacent) can occur; unifi-smart-archive coalesces them per camera and detection type into one archive window per stream.

## UniFi Protect Video (unifi-video-kafka)

Topic per camera: `unifi.protect.video.{camera_name}` (camera name sanitized: lowercase, spaces/dashes→underscores). **Retention: 30 minutes** (`retention.ms=1800000`); topics created by the service via Kafka Admin API. No maintainer cleanup needed.

Key: `{camera_id}:{timestamp}`. Value: raw JPEG frame bytes (binary, ~1 frame/sec).

| Header | Description |
|--------|-------------|
| schema_version | 1 |
| camera_id | Protect camera UUID |
| camera_name | Sanitized camera name |
| timestamp | Unix seconds |
| source | unifi-protect-video |
| content_type | image/jpeg |

## Person Detection (detect-person)

Key: `{person_name}:{image_timestamp}`. Published when classification confidence exceeds threshold.

| Topic | Description | Consumer |
|-------|-------------|----------|
| detect.person | Person classification results from CoreML | — |

**Headers**: `schema_version`, `camera_name`, `event_start`, `timestamp`, `source`.

**Body**: JSON with `person`, `confidence`, `alternatives` (array of `{person, confidence}`), `image_path`, `camera_name`, `event_start`, `image_timestamp`.

## Vehicle Detection (coreml-vehicle-detect)

Key: `vehicle:{image_timestamp}`. Published for each cropped vehicle image from coreml-smart-crop.

| Topic | Description | Consumer |
|-------|-------------|----------|
| detect.vehicle | Car make/model classification (CompCars-based Core ML) | — |

**Body**: JSON with `ts`, `image_path`, `top` (array of `{ label, confidence }`).

## Home Assistant Ecobee (homeassistant-kafka)

Key: `{friendly_name}:{timestamp}` (entity_id minus domain and topic-redundant suffix).

| Topic | Entity type |
|-------|-------------|
| homeassistant.ecobee.thermostat.climate | climate |
| homeassistant.ecobee.weather | weather |
| homeassistant.ecobee.sensor.temperature | sensor (temperature) |
| homeassistant.ecobee.sensor.humidity | sensor (humidity) |
| homeassistant.ecobee.sensor.presence | binary_sensor (occupancy/presence) |
| homeassistant.ecobee.sensor.battery | sensor (battery) |
| homeassistant.ecobee.other | Fallback |

## Home Assistant Command (consumed by homeassistant-command)

| Topic | Consumer group | Body |
|-------|----------------|------|
| homeassistant.command | homeassistant-command | JSON: `domain`, `service`, `entity_id`, `data` |

**Example body**: `{"domain":"climate","service":"set_temperature","entity_id":"climate.sneaux","data":{"temperature":72}}`  
**Services**: set_temperature, set_hvac_mode, set_preset_mode, set_fan_mode, turn_on, turn_off. Test: `scripts/homeassistant/send-command.sh`.

## Message structure

**Weather data headers**: `schema_version`, `lsid`, `timestamp`, `sensor_type`, `data_structure_type`. (Removed: station_id, station_id_uuid, category, product_name — use metadata.)  
**Body**: JSON; include `ts` for timestamp. Example (weather.iss): `{"temp":62.3,"hum":55.2,"wind_speed_last":5.0,"ts":1706140800}`.

**HA event headers**: `schema_version`, `entity_id`, `domain`, `timestamp`, `source`, `event_type`.  
**Body**: Full HA state_changed event JSON.

**UniFi Protect headers**: `schema_version`, `camera_id`, `camera_name`, `event_type`, `detection_type`, `timestamp`, `source`.  
**Body**: Raw Protect event JSON (e.g. id, modelKey, type, start, end, smartDetectTypes, camera).

See [kafka-standards.md](kafka-standards.md) for optimization and required headers.

## Consuming and monitoring

**CLI**: List: `docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092`. Describe: `--describe --topic weather.iss`. Consume: `kafka-console-consumer --topic weather.iss --from-beginning --max-messages 5`; add `--property print.headers=true` for headers.  
**Kafka UI**: http://localhost:8080 → Topics.

**Adding topics**: Publish with naming `namespace.category.subcategory`; include headers (e.g. timestamp, schema_version, entity/sensor id). Suggested patterns: `home.hvac.*`, `home.security.*`, `home.energy.*`.
