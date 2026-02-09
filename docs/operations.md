# Operations

> **Basics**: [CLAUDE.md](../CLAUDE.md). **Scripts**: [scripts/README.md](../scripts/README.md). This doc: advanced ops, maintenance, backfill, dev workflow.

## Quick reference

| Action | Command |
|--------|---------|
| Start | `./scripts/start-all.sh` \| `start-all.sh build` \| `start-all.sh clean` |
| Stop | `./scripts/stop-all.sh` \| `stop-all.sh clean` |
| Status | `./scripts/status.sh` |
| Logs | `./scripts/logs.sh [service]` |
| Restart | `./scripts/restart-all.sh [service]` |
| DB query | `./scripts/db/query.sh stats \| devices \| tags <lsid> \| recent \| orphans \| psql` |
| Migrate | `./scripts/db/migrate.sh status \| up \| down \| create <name>` |
| Backfill API→Kafka | `BACKFILL_START_TS=... BACKFILL_END_TS=... ./scripts/weatherlink/kafka-backfill.sh` |
| Backfill Kafka→DB | `./scripts/weatherlink/sql-backfill.sh [--metadata] [--topics ...]` |

## Service lifecycle

```bash
./scripts/start-all.sh              # Normal
./scripts/start-all.sh build        # Rebuild containers
./scripts/start-all.sh clean        # Remove volumes
./scripts/start-infra.sh            # Infra only (local dev)
./scripts/stop-all.sh               # Preserve data
./scripts/stop-all.sh clean         # Remove volumes
./scripts/restart-all.sh [service]
```

## Monitoring

- **Status**: `./scripts/status.sh` (containers, topics, DB stats, disk).
- **Logs**: `./scripts/logs.sh` or `./scripts/logs.sh weatherlink-kafka`; or `docker logs -f roach-weatherlink-kafka`.
- **Resources**: `docker stats`.
- **Health**: `docker ps` (healthy/starting/unhealthy). Startup order: Zookeeper → Kafka (20–30s) → PostgreSQL → apps.
- **Kafka UI**: http://localhost:8080 (topics, messages, consumer groups).

## Database

**Queries**: `./scripts/db/query.sh stats | devices | tags <lsid> | recent [lsid] | orphans | psql`

**Migrations**:
```bash
./scripts/db/migrate.sh status
./scripts/db/migrate.sh create add_new_column
# Edit scripts/db/migrations/NNN_*.up.sql and .down.sql
./scripts/db/migrate.sh up
./scripts/db/migrate.sh down   # Rollback (prompts)
```
Tracked in `schema_migrations`. Use IF EXISTS/IF NOT EXISTS; test rollback.

**Orphans**: `./scripts/db/query.sh orphans`. Reasons: `missing_device`, `failed_to_parse`, `failed_to_create_tag`, invalid metadata. Fix cause then `./scripts/db/reload-orphans.sh`. If missing_device: `./scripts/weatherlink/sql-backfill.sh --metadata` first.

## Backup and restore

**DB dump**:
```bash
docker exec roach-postgres pg_dump -U roach roach > backup-$(date +%Y%m%d).sql
docker exec roach-postgres pg_dump -U roach roach | gzip > backup-$(date +%Y%m%d).sql.gz
```
**Restore**: `cat backup.sql | docker exec -i roach-postgres psql -U roach -d roach` (or gunzip -c for .gz).

**Full backup**: `./scripts/stop-all.sh` → `tar -czf roach-backup-$(date +%Y%m%d).tar.gz data/` → `./scripts/start-all.sh`. Restore: stop, `rm -rf data/`, extract, start.

## Kafka

**Topics**:
```bash
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092
docker exec roach-kafka kafka-topics --describe --topic weather.iss --bootstrap-server localhost:29092
```

**Consume**:
```bash
docker exec roach-kafka kafka-console-consumer --bootstrap-server localhost:29092 --topic weather.iss --from-beginning --max-messages 10
# With headers/key/timestamp: --property print.headers=true --property print.timestamp=true --property print.key=true
```

**Consumer groups**:
```bash
docker exec roach-kafka kafka-consumer-groups --list --bootstrap-server localhost:29092
docker exec roach-kafka kafka-consumer-groups --describe --group weatherlink-sql-data --bootstrap-server localhost:29092
# Reset: --reset-offsets --to-earliest|--to-latest|--to-offset N --all-topics|--topic T:P --execute (stop group first)
```

**Broker**: `docker exec roach-kafka kafka-broker-api-versions --bootstrap-server localhost:29092`

## Maintenance

**Routine**: Daily: `status.sh`, `db/query.sh stats`, `db/query.sh orphans`, `du -sh data/*`. Weekly: grep logs for errors, topic sizes. Monthly: DB backup, full backup, disk trends.

**After code changes**: `./scripts/start-all.sh build` or `docker compose build <service>` then `./scripts/restart-all.sh <service>`.

**Credential rotation**: Edit `.env`, restart affected service(s). PostgreSQL: stop all, change POSTGRES_PASSWORD, remove postgres volume, start (backup first).

**Log rotation**: In docker-compose.yml: `logging: driver: json-file; options: max-size: "10m", max-file: "3"`.

## Backfill

**API → Kafka** (runs in weatherlink-kafka when KAFKA_BACKFILL_ENABLED=true):
```bash
BACKFILL_START_TS=$(date -v-24H +%s) BACKFILL_END_TS=$(date +%s) ./scripts/weatherlink/kafka-backfill.sh
# Or -v-7d, or explicit unix timestamps
```

**Kafka → DB**:
```bash
./scripts/weatherlink/sql-backfill.sh --metadata          # Metadata first (fresh DB)
./scripts/weatherlink/sql-backfill.sh                     # All data topics
./scripts/weatherlink/sql-backfill.sh --topics weather.iss,weather.barometer
./scripts/weatherlink/sql-backfill.sh --workers 16 --batch-size 1000
```
Fresh DB: `--metadata` then full backfill. Rebuild: stop weatherlink-sql, truncate tables in psql, `--metadata` then backfill, start weatherlink-sql.

## Development

**Local run**: `./scripts/start-infra.sh` → set KAFKA_BROKER=localhost:9092, POSTGRES_DSN, etc. → `cd services/<name> && go run main.go`.

**Debug**: `docker exec -it roach-weatherlink-kafka sh`; `docker exec roach-weatherlink-kafka env`; `nc -zv kafka 29092` from container; `docker logs --timestamps roach-weatherlink-kafka`.

## Docker

**Containers**: `docker ps`, `docker inspect roach-kafka`, `docker logs -f roach-kafka`, `docker stop/start/restart <container>`.

**Images**: `docker images`, `docker compose build <service>`, `docker image prune`.

**Network**: `docker network inspect roach-network`. Recreate: stop all, `docker network rm roach-network`, start all.

**Volumes**: `docker volume ls`, `docker system df -v`. Clean: `./scripts/stop-all.sh clean` or `docker volume rm roach_*`.

## Validation

**Config**: `docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml config` (and `config --quiet` for validate).

**Connectivity**: `docker exec roach-kafka kafka-broker-api-versions --bootstrap-server localhost:29092`; `docker exec roach-postgres pg_isready -U roach`; `docker exec roach-weatherlink-kafka nc -zv kafka 29092`.

**Data flow**: `./scripts/logs.sh weatherlink-kafka` (produce), `./scripts/logs.sh weatherlink-sql` (consume), `kafka-topics --list`, GetOffsetShell for topic count, `./scripts/db/query.sh stats` / `recent` / `orphans`.

## Tuning

- **Fetch**: Increase `FETCH_INTERVAL` (e.g. 10m) in env to reduce API/Kafka load.
- **Logging**: `LOG_LEVEL=debug|info|warn|error`.
- **weatherlink-sql**: `WORKER_POOL_SIZE`, `BATCH_SIZE`, `BATCH_FLUSH_INTERVAL_MS`.
- **Kafka** (infra): `KAFKA_NUM_IO_THREADS`, `KAFKA_LOG_FLUSH_INTERVAL_MS`, `KAFKA_COMPRESSION_TYPE=lz4`.
