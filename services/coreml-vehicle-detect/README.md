# coreml-vehicle-detect

Native macOS Swift service that watches `data/streams/coreml/vehicle` (cropped vehicle images from coreml-smart-crop), runs a **CompCars-based Core ML** make/model classifier, and publishes one JSON message per image to Kafka topic `detect.vehicle`.

**Runtime**: macOS (Apple Silicon), Swift 5.9+. Not Docker — requires Vision, Core ML, and Kafka (swift-kafka-client). Building may require macOS 15 SDK for the Kafka dependency.

## Flow

1. Watch `data/streams/coreml/vehicle` (recursive) for new `.jpg` files (FSEvents).
2. For each new file: run car classification (top-N); publish to `detect.vehicle` with key `vehicle:{image_timestamp}` and JSON body `ts`, `image_path`, `top: [{ label, confidence }]`.
3. If classification fails or returns empty, skip (debug log only).

## Model

The service requires a car recognition Core ML model at `CAR_MODEL_PATH` (default `./data/models/CarRecognition.mlmodel`). CompCars-based models (e.g. from [Core-ML-Car-Recognition](https://github.com/likedan/Core-ML-Car-Recognition)) are supported. Retries every 10s until model is available.

**Obtain the model:** Run `./scripts/models/download-car-model.sh` to download the pre-built model to `./data/models/CarRecognition.mlmodel` (and compiled `.mlmodelc`).

## Configuration

All via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `WATCH_DIR` | `./data/streams/coreml/vehicle` | Directory to watch for new vehicle crop images |
| `CAR_MODEL_PATH` | `./data/models/CarRecognition.mlmodel` | Path to car recognition Core ML model |
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker (host, not Docker) |
| `KAFKA_TOPIC` | `detect.vehicle` | Topic for vehicle detection messages |
| `DEBOUNCE_SECONDS` | `1.0` | Debounce delay before processing a new file |
| `LOG_LEVEL` | `info` | `debug` or `info` |
| `TOP_N` | `5` | Number of top class labels to include in each message |

## Scripts

From project root (see [scripts/coreml-vehicle-detect](../../scripts/coreml-vehicle-detect)):

| Script | Description |
|--------|-------------|
| `build/build.sh [release\|clean]` | Build Swift package (release = release build; clean = full rebuild) |
| `run/detect.sh` | Run in foreground (Ctrl+C to stop) |
| `run/start.sh` | Start as background daemon |
| `run/stop.sh` | Stop daemon |
| `run/status.sh` | Process and directory diagnostics |
| `run/logs.sh [lines]` | Tail log file |

Logs: `data/logs/coreml-vehicle-detect.log`. PID: `data/logs/coreml-vehicle-detect.pid`.

## Dependencies

- **Input**: Vehicle crops from coreml-smart-crop: `data/streams/coreml/vehicle/{timestamp}.jpg`.
- **Output**: Kafka topic `detect.vehicle` (see [docs/kafka-topics.md](../../docs/kafka-topics.md)).
