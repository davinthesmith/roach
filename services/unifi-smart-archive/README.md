# unifi-smart-archive

Consumes UniFi Protect **smart** events from Kafka and copies the corresponding time-window of JPEG frames (from unifi-video-jpg output) into a long-term archive. Only events that include an `end` time are archived. Archive files are retained for a configurable number of days (default 10).

## Flow

1. Consume `unifi.protect.smart` (key: `camera_name:timestamp`, value: event JSON with `id`, `start`, `end`, `smartDetectTypes`). You may receive multiple messages per event; the final one has `end`.
2. When an event has `end`, schedule a copy job for `end + trail + copyDelay` (so trailing frames exist).
3. If an event has only `start` (waiting for final message), we track it. If no message for that event within `EVENT_END_TIMEOUT`, we stop waiting and do not archive it (e.g. connection problem).
4. Copy worker: for each due job, copy frames in `[start - lead, end + trail]` from source dir to archive.
5. Retention: periodically delete archive event directories older than `ARCHIVE_RETENTION_DAYS`.

## Path layout

- **Source** (read): `{SOURCE_DIR}/{camera_name}/{unix_seconds}.jpg` (same as unifi-video-jpg).
- **Archive** (write): `{ARCHIVE_DIR}/smart/{detection_type}/{camera_name}/{start_seconds}/{timestamp}.jpg`  
  Example: `./data/streams/unifi/protect/smart/person/back_patio_left/1770691938/1770691690.jpg`

## Configuration (env)

| Variable | Default | Description |
|----------|---------|-------------|
| KAFKA_BROKER | kafka:29092 | Kafka bootstrap servers |
| KAFKA_TOPIC | unifi.protect.smart | Topic to consume |
| KAFKA_CONSUMER_GROUP | unifi-smart-archive | Consumer group ID |
| EVENT_END_TIMEOUT | 1m | If no message for a given event within this duration, stop waiting for end (do not archive that event) |
| SOURCE_DIR | /data/streams/unifi/jpg | Base dir of camera JPEGs (read-only; same as unifi-video-jpg output) |
| ARCHIVE_DIR | /data/streams/unifi/protect | Base dir for long-term archive |
| LEAD_SECONDS | 60 | Seconds before event start to include |
| TRAIL_SECONDS | 60 | Seconds after event end to include |
| COPY_DELAY_SECONDS | 5 | Extra seconds after end+trail before running copy |
| ARCHIVE_RETENTION_DAYS | 10 | Delete archive content older than this |
| WORKER_INTERVAL | 10s | Copy and retention check interval |
| LOG_LEVEL | info | Log level (e.g. debug) |

## Failure behavior

- **No follow-up for an event**: If we're waiting for the final message (with `end`) for an event and receive no message for that event within `EVENT_END_TIMEOUT`, we stop waiting and do not archive that event. The process keeps running.
- **Kafka consumer error**: Process exits (non-zero). No indefinite retry.
- **Commit error**: Process exits (non-zero).

## Dependencies

- **Kafka**: Consumes `unifi.protect.smart`; depends on Kafka being healthy.
- **Filesystem**: Reads from the same directory as unifi-video-jpg (shared volume); writes to a separate archive volume. No process dependency on unifi-video-jpg.

## Local run

```bash
cd services/unifi-smart-archive
SOURCE_DIR=./data/streams/unifi/jpg ARCHIVE_DIR=./data/streams/unifi/protect \
  KAFKA_BROKER=localhost:9092 go run .
```
