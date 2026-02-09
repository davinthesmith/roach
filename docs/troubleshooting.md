# Troubleshooting

> **Common issues**: [CLAUDE.md](../CLAUDE.md). **Ops**: [operations.md](operations.md). **Scripts**: [scripts/README.md](../scripts/README.md). This doc: full problem-solving and edge cases.

## First steps

```bash
./scripts/status.sh
./scripts/logs.sh [service]
docker info
df -h && du -sh data/*
```

## By symptom

### Services won't start
**Check**: `./scripts/status.sh`, `./scripts/logs.sh`, `docker ps -a`, `lsof -i :9092 :2181 :8080 :5432`, `df -h`, `.env` (cat .env | grep WEATHERLINK).

**Causes**: Port conflict, low disk, Docker down, invalid .env.

**Fix**: Free ports or change in compose; free disk; restart Docker; fix .env and restart.

### Health check failing / containers restarting
**Check**: `docker ps`, `docker inspect roach-kafka | grep -A 10 Health`, `./scripts/logs.sh kafka`.

**Fix**: Kafka needs 20–60s. Use `./scripts/start-all.sh` (retry). If persistent: `./scripts/stop-all.sh clean` then `./scripts/start-all.sh`.

### Connection refused (services → Kafka/PostgreSQL)
**Cause**: Broker or DB not ready when service started.

**Fix**: Wait for `(healthy)` in `docker ps`, then `./scripts/restart-all.sh weatherlink-kafka` (or weatherlink-sql).

### API auth (401) / no data publishing
**Check**: `docker exec roach-weatherlink-kafka env | grep WEATHERLINK`, `./scripts/logs.sh weatherlink-kafka`, `kafka-topics --list`.

**Fix**: Correct WEATHERLINK_API_KEY/SECRET/STATION_ID in .env, `./scripts/restart-all.sh weatherlink-kafka`. Test API: `curl -v "https://api.weatherlink.com/v2/current/[station-id]?api-key=[key]"`.

### No data in PostgreSQL (Kafka has data)
**Check**: `./scripts/logs.sh weatherlink-sql`, `./scripts/db/query.sh stats`, `./scripts/db/query.sh orphans`.

**Fix**: Often missing devices. `./scripts/weatherlink/sql-backfill.sh --metadata`, then `./scripts/db/reload-orphans.sh`, then `./scripts/restart-all.sh weatherlink-sql`.

### Kafka broker not responding
**Check**: `./scripts/status.sh`, `./scripts/logs.sh kafka`, `docker exec roach-kafka kafka-broker-api-versions --bootstrap-server localhost:29092`.

**Fix**: `./scripts/restart-all.sh kafka` or `./scripts/stop-all.sh clean` then `./scripts/start-all.sh`.

### Cluster ID mismatch (InconsistentClusterIdException)
**Cause**: Kafka cluster ID changed, Zookeeper has old ID.

**Fix**: `./scripts/stop-all.sh`, `rm -rf data/kafka/* data/zookeeper/*`, `./scripts/start-all.sh`. (All Kafka data lost.)

### Topics not auto-created
**Check**: `kafka-configs --entity-type brokers --entity-default --describe` for `auto.create.topics.enable=true`. Create manually if needed: `kafka-topics --create --topic weather.iss --partitions 1 --replication-factor 1 --bootstrap-server localhost:29092`.

### Kafka / disk full
**Check**: `df -h`, `du -sh data/kafka data/zookeeper data/postgres`.

**Fix**: Short-term: stop, `rm -rf data/kafka/* data/zookeeper/*` (or postgres), start. Long-term: set retention in docker-compose.infrastructure.yml, e.g. `KAFKA_LOG_RETENTION_MS: 2592000000` (30 days).

### Kafka UI not loading (localhost:8080)
**Check**: `docker logs roach-kafka-ui`, `docker ps | grep kafka-ui`, `lsof -i :8080`.

**Fix**: Wait for Kafka healthy (~30s), then `docker restart roach-kafka-ui`. If "Retrying to fetch metadata": verify listeners in docker-compose.infrastructure.yml (PLAINTEXT/PLAINTEXT_HOST, kafka:29092, localhost:9092); clean restart of infra if config was wrong.

### PostgreSQL connection refused
**Check**: `docker logs roach-postgres`, `docker exec roach-postgres pg_isready -U roach`, `docker exec roach-weatherlink-sql env | grep POSTGRES_DSN`.

**Fix**: `docker restart roach-postgres`; fix DSN if wrong.

### Migration failures
**Check**: `./scripts/db/migrate.sh status`, `SELECT * FROM schema_migrations;` in psql, `docker logs roach-postgres`.

**Fix**: Fix SQL in migration; if file was edited, checksum in schema_migrations may need update. Run migrations manually if necessary.

### Empty tag units/descriptions
**Check**: `SELECT COUNT(*) FROM tags WHERE unit IS NOT NULL;`, `SELECT COUNT(*) FROM sensor_catalog;`. Catalog topic should have messages.

**Fix**: Use version with catalog filtering; ensure weather.metadata.catalog is produced and consumed.

### Data not persisting (PostgreSQL)
**Check**: `docker inspect roach-postgres | grep -A 10 Mounts`. Volume should be `./data/postgres:/var/lib/postgresql/data` in compose.

### query.sh "not a TTY"
**Cause**: Script uses `docker exec -it`; non-interactive env has no TTY.

**Fix**: Call psql directly: `docker exec roach-postgres psql -U roach -d roach -c "SELECT COUNT(*) FROM devices;"`. For scripted use: `-t -A` (no headers, unaligned).

### Services can't communicate (network)
**Check**: `docker network ls | grep roach`, `docker network inspect roach-network`.

**Fix**: `docker compose down`, `docker network rm roach-network`, `./scripts/start-all.sh`.

### External access (host → Kafka/PostgreSQL)
**Check**: `nc -zv localhost 9092`, `nc -zv localhost 5432`, `docker ps` for port mappings.

**Fix**: Confirm port mappings in docker-compose (9092, 5432 exposed).

### High CPU / memory
**Check**: `docker stats`. Causes: compaction, short FETCH_INTERVAL, many consumers.

**Fix**: Increase FETCH_INTERVAL (e.g. 10m); limit container memory in compose (e.g. deploy.resources.limits.memory).

## Debug

**Verbose logs**: In docker-compose.yml set `LOG_LEVEL=debug` for the service, restart.

**Shell in container**: `docker exec -it roach-weatherlink-kafka sh`; then `env`, `nc -zv kafka 29092`, `nc -zv postgres 5432`.

**Collect info**: `./scripts/status.sh`, `docker compose logs`, `docker compose config`, `docker ps -a`, `docker network inspect roach-network` → save to file.

## Recovery

**Full reset** (deletes all data): `docker compose down -v`, `docker network prune -f`, `docker volume prune -f`, `rm -rf data/`, `./scripts/start-all.sh`. Backup first if needed.

**Kafka only**: `./scripts/stop-all.sh`, `rm -rf data/kafka/* data/zookeeper/*`, `./scripts/start-all.sh`.

**PostgreSQL only**: `./scripts/stop-all.sh`, `rm -rf data/postgres/*`, `./scripts/start-all.sh`, `./scripts/db/migrate.sh up`.
