# coreml-vehicle-detect

Swift service that watches `data/streams/coreml/vehicle`, runs a CompCars-based Core ML make/model classifier, and publishes one message per image to Kafka topic `detect.vehicle`.

## Flow

- **Watch**: `data/streams/coreml/vehicle` (FSEvents, recursive) for new `.jpg` files.
- **Classify**: Core ML car model (e.g. CarRecognition.mlmodel); top-N labels and confidences.
- **Kafka**: One message per image. Key `vehicle:{image_timestamp}`. Body: JSON with `ts`, `image_path`, `top` (array of `{ label, confidence }`).

## Model

Default path: `./data/models/CarRecognition.mlmodel`. Run `./scripts/models/download-car-model.sh` to download the pre-built model from [Core-ML-Car-Recognition](https://github.com/likedan/Core-ML-Car-Recognition) (saved under `data/models/`). Retries every 10s until model is available.

## Config (env)

| Variable | Default |
|----------|---------|
| `WATCH_DIR` | `./data/streams/coreml/vehicle` |
| `CAR_MODEL_PATH` | `./data/models/CarRecognition.mlmodel` |
| `KAFKA_BROKER` | `localhost:9092` |
| `KAFKA_TOPIC` | `detect.vehicle` |
| `DEBOUNCE_SECONDS` | `1.0` |
| `LOG_LEVEL` | `info` |
| `TOP_N` | `5` |

## Scripts

From project root: `./scripts/coreml-vehicle-detect/build/build.sh`, `run/detect.sh`, `run/start.sh`, `run/stop.sh`, `run/status.sh`, `run/logs.sh`. See [services/coreml-vehicle-detect/README.md](../services/coreml-vehicle-detect/README.md).
