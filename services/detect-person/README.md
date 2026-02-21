# detect-person

Native macOS service for person classification using CoreML and CreateML. Trains a model from labeled images and classifies people detected in UniFi Protect smart archive frames, publishing results to Kafka.

**Runtime**: macOS (Apple Silicon), Swift 5.9+. Not Docker — requires CoreML/CreateML frameworks.

## Modes

### Train

Train an image classifier from labeled person directories:

```bash
./scripts/detect-person/train/train.sh
```

Training data layout (`data/train/`):
```
data/train/
├── john_doe/
│   ├── img001.jpg
│   ├── img002.jpg
│   └── ...
└── jane_doe/
    ├── img001.jpg
    └── ...
```

Each subdirectory name becomes a classification label. Model saved to `data/models/detect-person/PersonClassifier.mlmodelc`.

### Detect

Watch for new images and classify:

```bash
# Foreground (interactive)
./scripts/detect-person/run/detect.sh

# Background daemon
./scripts/detect-person/run/start.sh
./scripts/detect-person/run/stop.sh

# Auto-start at login (survives reboot)
./scripts/detect-person/launchd/install-launchd.sh   # install
./scripts/detect-person/launchd/uninstall-launchd.sh # remove
```

Watches `data/streams/unifi/protect/smart/person/` (from unifi-smart-archive). When a new `.jpg` appears, runs classification and publishes to `detect.person` Kafka topic if confidence exceeds threshold.

## Configuration

All via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker (external listener) |
| `KAFKA_TOPIC` | `detect.person` | Output topic |
| `WATCH_DIR` | `./data/streams/unifi/protect/smart/person` | Directory to watch for new images |
| `TRAIN_DIR` | `./data/train` | Labeled training data |
| `MODEL_DIR` | `./data/models/detect-person` | Model storage |
| `CONFIDENCE_THRESHOLD` | `0.7` | Minimum confidence for Kafka publish |
| `MAX_ALTERNATIVES` | `5` | Max alternative matches in message |
| `DEBOUNCE_SECONDS` | `1.0` | Wait before processing new file |
| `LOG_LEVEL` | `info` | `debug` or `info` |

## Kafka Message

**Topic**: `detect.person`  
**Key**: `{person}:{image_timestamp}`

**Headers**: `schema_version`, `camera_name`, `event_start`, `timestamp`, `source`

**Body**:
```json
{
  "person": "john_doe",
  "confidence": 0.92,
  "alternatives": [
    {"person": "jane_doe", "confidence": 0.05}
  ],
  "image_path": "smart/person/back_patio_left/1770691938/1770691690.jpg",
  "camera_name": "back_patio_left",
  "event_start": 1770691938,
  "image_timestamp": 1770691690
}
```

## Scripts

Organized under `scripts/detect-person/` (run from project root):

| Path | Description |
|------|-------------|
| `build/build.sh [release]` | Build Swift package |
| `train/train.sh [options]` | Train model from labeled images |
| `run/detect.sh` | Run detection foreground |
| `run/start.sh` | Start as background daemon |
| `run/stop.sh` | Stop background daemon |
| `run/status.sh` | Process, model, and data diagnostics |
| `run/logs.sh [lines]` | Tail log file |
| `launchd/install-launchd.sh` | Install LaunchAgent (start at login, restart on crash/reboot) |
| `launchd/uninstall-launchd.sh` | Remove LaunchAgent |

## Prerequisites

- macOS 14+ with Apple Silicon
- Swift 5.9+ (`xcode-select --install`)
- `openssl@3` (`brew install openssl@3`) — required by swift-kafka-client/librdkafka
- Kafka infrastructure running (`./scripts/start-infra.sh`)
- Trained model (run `train` before `detect`)
