# coreml-smart-crop

Native macOS Swift service that watches the UniFi Protect **smart** archive (person, package, animal, vehicle), runs a **YOLO Core ML** object detector, crops each image to the best-matching allowed detection, and writes JPEGs to `data/streams/coreml/{person|package|animal|vehicle}/`.

**Runtime**: macOS (Apple Silicon), Swift 5.9+. Not Docker — requires Vision and Core ML.

## Flow

1. Watch `data/streams/unifi/protect/smart` (recursive) for new `.jpg` files under `smart/person/`, `smart/package/`, `smart/animal/`, `smart/vehicle/` (FSEvents).
2. For each new file: infer event type from path; run YOLO detection; filter by allowlist for that type (e.g. person: `person`; vehicle: `car`, `truck`, `bus`, `motorcycle`; animal: `dog`, `cat`, `horse`, etc.; package: `backpack`, `handbag`, `suitcase`); pick highest-confidence allowed detection; crop to that bbox; write to `{COREML_OUTPUT_DIR}/{type}/{image_timestamp}.jpg`.
3. If no allowed detection, skip (debug log only).

## Model

The service requires a YOLO model in Core ML format at `YOLO_MODEL_PATH` (default `./data/models/yolo.mlpackage`). Retries every 10s until model is available.

**Obtain the model:** From project root run `./scripts/models/download-yolo.sh`, or export manually:

```bash
pip install ultralytics
yolo export model=yolo11n.pt format=coreml nms=True
mkdir -p data/models && mv yolo11n.mlpackage data/models/yolo.mlpackage
```

See [scripts/models/download-yolo.sh](../../scripts/models/download-yolo.sh) and [docs/coreml-smart-crop.md](../../docs/coreml-smart-crop.md).

## Configuration

All via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCH_ROOT` | `./data/streams/unifi/protect/smart` | Root directory to watch; events under `person/`, `package/`, `animal/`, `vehicle/` are processed |
| `COREML_OUTPUT_DIR` | `./data/streams/coreml` | Base output directory; crops go to `{COREML_OUTPUT_DIR}/{type}/` |
| `YOLO_MODEL_PATH` | `./data/models/yolo.mlpackage` | Path to YOLO Core ML model (`.mlpackage`) |
| `DEBOUNCE_SECONDS` | `1.0` | Debounce delay before processing a new file |
| `LOG_LEVEL` | `info` | `debug` or `info` |

## Scripts

From project root (see [scripts/coreml-smart-crop](../../scripts/coreml-smart-crop)):

| Script | Description |
|--------|-------------|
| `build/build.sh [release\|clean]` | Build Swift package (release = release build; clean = full rebuild) |
| `run/detect.sh` | Run in foreground (Ctrl+C to stop) |
| `run/start.sh` | Start as background daemon |
| `run/stop.sh` | Stop daemon |
| `run/status.sh` | Process and directory diagnostics |
| `run/logs.sh [lines]` | Tail log file |

Logs: `data/logs/coreml-smart-crop.log`. PID: `data/logs/coreml-smart-crop.pid`.

## Dependencies

- **Input**: UniFi Protect smart archive (from unifi-smart-archive): `smart/{person|package|animal|vehicle}/{camera_name}/{event_start}/{timestamp}.jpg`.
- **Output**: `data/streams/coreml/{person|package|animal|vehicle}/{timestamp}.jpg` (one crop per source image when an allowed class is detected).
