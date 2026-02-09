# weatherlink-sql-backfill

One-shot Kafka → PostgreSQL backfill. Replays configured topics and materializes into the same schema as `weatherlink-sql`. Separate consumer group; exits when done.

## When to Use

- Kafka has full history but DB is behind
- Real-time materializer was down; need to catch up
- Rebuild DB from Kafka

## Architecture

```
Kafka (data + optional metadata) → weatherlink-sql-backfill → PostgreSQL
                                      ↓
                              Worker pool + batch writer (COPY)
                              ON CONFLICT DO NOTHING (dedup)
```

**Flow**: Connect PostgreSQL → Load devices, tags, catalog into cache → Optional metadata phase (sensors → catalog → station) → Process data topics in parallel (one goroutine per topic, partition 0) → Worker pool + batch writer → Exit when all topics processed or END_OFFSET reached.

## Configuration

**Required**:

```bash
POSTGRES_DSN=host=postgres port=5432 user=roach password=xxx dbname=roach sslmode=disable
```

**Optional** (env or CLI; CLI overrides env):

```bash
KAFKA_BROKER=kafka:29092
TOPICS=weather.iss,weather.barometer,weather.indoor,weather.health   # default
START_OFFSET=-2    # -2=earliest, -1=latest, or offset (default -2)
END_OFFSET=-1      # -1=latest, or offset (default -1)
WORKER_POOL_SIZE=8
BATCH_SIZE=500
BATCH_FLUSH_INTERVAL_MS=1000
DB_POOL_MAX_CONNS=10
INCLUDE_METADATA=false
LOG_LEVEL=info
```

**CLI flags**: `--topics`, `--start-offset`, `--end-offset`, `--workers`, `--batch-size`, `--metadata`.

## Run

```bash
# From repo root (script sets env and runs container)
./scripts/weatherlink/sql-backfill.sh
./scripts/weatherlink/sql-backfill.sh --topics weather.iss,weather.indoor
./scripts/weatherlink/sql-backfill.sh --metadata

# Standalone
cd services/weatherlink-sql-backfill
go build -o weatherlink-sql-backfill
export POSTGRES_DSN=... KAFKA_BROKER=...
./weatherlink-sql-backfill [--topics ...] [--metadata] ...
```

## Behavior

- **Data topics**: One reader per topic (partition 0). Offsets from START_OFFSET; stop at END_OFFSET if set, else when read timeout and no messages.
- **Metadata phase** (if `--metadata` / INCLUDE_METADATA): Process `weather.metadata.sensors`, then `weather.metadata.catalog`, then `weather.metadata.station`; reload cache.
- **Orphans**: Failed messages written to `orphaned_messages`.
- **Progress**: Logged every 10s when END_OFFSET is set; final summary at exit.

## Troubleshooting

| Issue | Action |
|-------|--------|
| Won't start | PostgreSQL and Kafka up; valid POSTGRES_DSN |
| No writes | Ensure devices exist; run with `--metadata` first if needed; check `orphaned_messages` |
