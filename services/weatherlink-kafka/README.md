# weatherlink-kafka

Polls WeatherLink v2 API and publishes current conditions plus metadata to Kafka. Optional historical backfill.

## Architecture

```
WeatherLink API → weatherlink-kafka → Kafka
                      ↓
              Metadata (deduped) + optional backfill
```

**Flow**: Load config → Connect Kafka → Fetch metadata → Publish metadata (key-deduped) → Start fetch loop every `FETCH_INTERVAL` → If backfill enabled, run parallel 24h-window workers → Periodic metadata refresh every `METADATA_FETCH_INTERVAL`.

## Topics

| Topic | Content |
|-------|---------|
| `weather.iss` | Outdoor (ISS) |
| `weather.barometer` | Barometric pressure |
| `weather.indoor` | Indoor (INSIDE TEMP/HUM) |
| `weather.health` | Console health |
| `weather.other` | Unknown categories |
| `weather.metadata.sensors` | Sensor config (key: `lsid:weekStart`) |
| `weather.metadata.catalog` | Catalog (key: `sensor_type:max_data_structure_type`) |
| `weather.metadata.station` | Station info (key: `station_id:weekStart`) |

Dedup: in-memory key cache from Kafka scan at startup; skip publish if key exists. Metadata keys include week start (UTC) for once-per-week publish.

## Configuration

**Required** (env or `.env`):

```bash
WEATHERLINK_API_KEY=...
WEATHERLINK_API_SECRET=...
WEATHERLINK_STATION_ID=...
KAFKA_BROKER=kafka:29092
```

**Optional**:

```bash
FETCH_INTERVAL=5m              # Current conditions poll (default 5m)
METADATA_FETCH_INTERVAL=168h   # Metadata refresh (default 168h = 1 week)
LOG_LEVEL=info

# Backfill (default enabled). When enabled, START < END required; END=0 means now.
KAFKA_BACKFILL_ENABLED=true
BACKFILL_START_TS=0            # Unix seconds
BACKFILL_END_TS=0              # 0 = now
```

Internal (code defaults): `BackfillRequestPerSecond=8`, `BackfillParallelWorkers=4`.  
`POSTGRES_DSN` is read but not used by this service.

## Run

```bash
# From repo root
docker compose up weatherlink-kafka

# Or standalone
cd services/weatherlink-kafka
export WEATHERLINK_API_KEY=... WEATHERLINK_API_SECRET=... WEATHERLINK_STATION_ID=...
export KAFKA_BROKER=localhost:9092
go run main.go
```

## Message Format

**Weather message headers**: `schema_version`, `lsid`, `timestamp`, `sensor_type`, `data_structure_type`.  
**Body**: JSON object with sensor fields (e.g. `temp`, `hum`, `ts`).  
**Key**: `lsid:timestamp` (string).

**Metadata**: JSON per [WeatherLink API](https://weatherlink.github.io/v2-api/) (sensors, catalog, station).

## Extending

- **New topic for category**: Edit `GetTopicForCategory()` in `util/topic.go`; add case and return `weather.<name>`.
- **Fetch interval**: Any Go duration for `FETCH_INTERVAL` (e.g. `1m`, `15m`, `1h`).

## Troubleshooting

| Issue | Action |
|-------|--------|
| Won't start | Check API credentials, Kafka reachable (wait ~30s after Kafka start), valid station ID |
| No data | Verify credentials and station ID; `docker compose logs -f weatherlink-kafka` |
| 401 | Invalid API key/secret |
| 404 | Invalid station ID |
| 429 | Rate limited; reduce fetch interval or backfill range |

## Related

- **weatherlink-sql**: Kafka → PostgreSQL materialization
- **weatherlink-sql-backfill**: Replay Kafka → PostgreSQL
