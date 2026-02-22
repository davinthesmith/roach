# coreml-smart-crop

Swift service that watches the UniFi Protect **smart** archive (person, package, animal, vehicle), runs a **YOLO Core ML** object detector, crops to the best-matching allowed detection per type, and writes to `data/streams/coreml/{person|package|animal|vehicle}/`.

## Flow

- **Watch**: `data/streams/unifi/protect/smart` (FSEvents, recursive). Event type is inferred from path: segment under `smart/` must be `person`, `package`, `animal`, or `vehicle`.
- **Detect**: YOLO Core ML model via `VNCoreMLRequest`; all detections then filtered by allowlist per type.
- **Allowlists**: person: `person`; vehicle: `car`, `truck`, `bus`, `motorcycle`; animal: `dog`, `cat`, `horse`, `sheep`, `cow`, `elephant`, `bear`, `zebra`, `giraffe`, `bird`; package: `backpack`, `handbag`, `suitcase` (COCO has no "package" class).
- **Crop**: Highest-confidence allowed detection; normalized bbox → image rect; crop and write JPEG to `{COREML_OUTPUT_DIR}/{type}/{image_timestamp}.jpg`.
- **Skip**: If path type not in allowlist or no allowed detection (debug log only).

## Model

Default path: `./data/models/yolo.mlpackage`. Obtain via `./scripts/models/download-yolo.sh` or export with Ultralytics:

```bash
pip install ultralytics
yolo export model=yolo11n.pt format=coreml nms=True
mkdir -p data/models && mv yolo11n.mlpackage data/models/yolo.mlpackage
```

Retries every 10s until model is available.

## Config (env)

| Variable | Default |
|----------|---------|
| `WATCH_ROOT` | `./data/streams/unifi/protect/smart` |
| `COREML_OUTPUT_DIR` | `./data/streams/coreml` |
| `YOLO_MODEL_PATH` | `./data/models/yolo.mlpackage` |
| `DEBOUNCE_SECONDS` | `1.0` |
| `LOG_LEVEL` | `info` |

## Scripts

From project root: `./scripts/coreml-smart-crop/build/build.sh [release|clean]`, `run/detect.sh`, `run/start.sh`, `run/stop.sh`, `run/status.sh`, `run/logs.sh`. See [services/coreml-smart-crop/README.md](../services/coreml-smart-crop/README.md).
