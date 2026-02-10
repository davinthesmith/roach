# ubiquiti-video-jpg

UniFi Protect RTSPS video streams → filesystem. Captures **1 frame per second** per camera via ffmpeg and writes JPEG frames to `JPG_OUTPUT_DIR` with configurable **retention** (files older than `RETENTION` are deleted automatically).

Only one video stream service should run at a time: use either **ubiquiti-video-jpg** (files) or **ubiquiti-video-kafka** (Kafka). The default docker-compose runs ubiquiti-video-jpg and has ubiquiti-video-kafka commented out.

## How it works

1. Fetches camera list from UniFi Protect API (`GET /cameras`)
2. For each camera, requests an RTSPS stream URL (`POST /cameras/{id}/rtsps-stream`)
3. Starts one ffmpeg process per camera: decodes RTSPS → outputs 1 MJPEG frame/sec to stdout
4. Parses JPEG frames from ffmpeg stdout (SOI/EOI markers) and writes each to `{JPG_OUTPUT_DIR}/{camera_name}/{timestamp}.jpg`
5. Retention cleanup runs every 1 minute per camera: deletes files older than `RETENTION`
6. On stream failure: backs off, re-fetches RTSPS URL (token may expire), restarts ffmpeg

## Output layout

- **Directory**: `{JPG_OUTPUT_DIR}/{camera_name}/` (e.g. `./data/streams/unifi/jpg/courtyard/`)
- **Files**: `{unix_timestamp}.jpg` (e.g. `1707123456.jpg`)
- Camera names are sanitized: lowercase, spaces and dashes → underscores.

## Configuration

| Variable        | Required | Default                     | Description                                      |
|-----------------|----------|-----------------------------|--------------------------------------------------|
| UNIFI_HOST      | Yes      | —                           | NVR URL (e.g. `https://192.168.1.1`)             |
| UNIFI_API_KEY   | Yes      | —                           | UniFi integration API key                        |
| JPG_OUTPUT_DIR  | No       | `./data/streams/unifi/jpg` | Base directory for JPEG frames                   |
| RETENTION       | No       | `10m`                       | Delete files older than this (e.g. `30m`, `1h`)  |
| LOG_LEVEL       | No       | info                        | Log level (info, debug)                          |
| RECONNECT_BACKOFF | No     | 1s,5s,30s                   | Comma-separated backoff durations                |

## Running

### With Docker Compose (recommended)

The service is defined in `docker-compose.yml`. Start with the rest of the stack:

```bash
./scripts/start-all.sh build
```

Frames are written to `./data/streams/unifi/jpg` on the host (mounted from the container).

### Standalone (development)

```bash
cd services/ubiquiti-video-jpg
UNIFI_HOST=https://192.168.1.1 UNIFI_API_KEY=... JPG_OUTPUT_DIR=./data/streams/unifi/jpg go run .
```

Requires ffmpeg installed locally.

## Troubleshooting

| Issue           | Action                                                                 |
|-----------------|------------------------------------------------------------------------|
| ffmpeg not found | Ensure ffmpeg is installed: `apk add ffmpeg` (Docker) or `brew install ffmpeg` (local) |
| RTSPS 401/403   | Check UNIFI_API_KEY is valid; regenerate in UniFi console              |
| No frames written | Check ffmpeg stderr in debug logs (`LOG_LEVEL=debug`); verify RTSPS URL: `ffmpeg -rtsp_transport tcp -i "rtsps://..." -frames:v 1 test.jpg` |
| Stream keeps reconnecting | RTSPS tokens expire; this is normal. Check backoff logs. If too frequent, check NVR connectivity |
