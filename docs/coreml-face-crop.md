# coreml-face-crop

Swift service that watches `data/streams/coreml/person`, detects faces with Apple Vision (`VNDetectFaceRectanglesRequest`), and writes each face crop to `data/streams/coreml/faces/`.

## Flow

- **Watch**: `data/streams/coreml/person` (FSEvents).
- **Detect**: `VNDetectFaceRectanglesRequest` (Vision built-in).
- **Crop**: For each face observation, convert normalized rect to image coordinates; write JPEG. Naming: one face → `{original_basename}.jpg`; multiple → `{base}_0.jpg`, `{base}_1.jpg`, ...
- **Skip**: If no face detected (debug log only).

## Config (env)

| Variable | Default |
|----------|---------|
| `WATCH_DIR` | `./data/streams/coreml/person` |
| `FACES_DIR` | `./data/streams/coreml/faces` |
| `DEBOUNCE_SECONDS` | `1.0` |
| `LOG_LEVEL` | `info` |

## Scripts

From project root: `./scripts/coreml-face-crop/build/build.sh [release|clean]`, `run/detect.sh`, `run/start.sh`, `run/stop.sh`, `run/status.sh`, `run/logs.sh`. See [services/coreml-face-crop/README.md](../services/coreml-face-crop/README.md).
