# coreml-face-crop

Native macOS Swift service that watches `data/streams/coreml/person` (person crops from coreml-smart-crop), detects faces with Vision (`VNDetectFaceRectanglesRequest`), and writes each face crop to `data/streams/coreml/faces/`. Naming: one face per image → `{timestamp}.jpg`; multiple faces → `{timestamp}_0.jpg`, `{timestamp}_1.jpg`, ...

**Runtime**: macOS (Apple Silicon), Swift 5.9+. Not Docker — requires Vision framework.

## Flow

1. Watch `data/streams/coreml/person` for new `.jpg` files (FSEvents).
2. For each new file: run face detection; for each face, crop to bounding box and write to `{FACES_DIR}/{name}.jpg` (name as above).
3. If no face is detected, skip (debug log only).

## Configuration

All via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCH_DIR` | `./data/streams/coreml/person` | Directory to watch for new person crop images |
| `FACES_DIR` | `./data/streams/coreml/faces` | Output directory for face crops |
| `DEBOUNCE_SECONDS` | `1.0` | Debounce delay before processing a new file |
| `LOG_LEVEL` | `info` | `debug` or `info` |

## Scripts

From project root (see [scripts/coreml-face-crop](../../scripts/coreml-face-crop)):

| Script | Description |
|--------|-------------|
| `build/build.sh [release]` | Build Swift package |
| `run/detect.sh` | Run in foreground (Ctrl+C to stop) |
| `run/start.sh` | Start as background daemon |
| `run/stop.sh` | Stop daemon |
| `run/status.sh` | Process and directory diagnostics |
| `run/logs.sh [lines]` | Tail log file |

Logs: `data/logs/coreml-face-crop.log`. PID: `data/logs/coreml-face-crop.pid`.

## Dependencies

- **Input**: Person crops from coreml-smart-crop: `data/streams/coreml/person/*.jpg`.
- **Output**: `data/streams/coreml/faces/{base}.jpg` or `{base}_0.jpg`, `{base}_1.jpg`, ...
