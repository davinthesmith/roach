# Kafka Standards and Optimization Guide

> **Overview**: [CLAUDE.md](../CLAUDE.md). **Topic schemas**: [kafka-topics.md](kafka-topics.md). This doc: producer/consumer standards, message structure, storage optimization.

**Applies to**: All Kafka producers and consumers in ROACH. **Goal**: 70–80% storage reduction with reliability.

## Summary: Implemented (weatherlink-kafka)

| Optimization | Effect | Status |
|--------------|--------|--------|
| LZ4 compression | 60–70% size | ✅ |
| Header reduction | ~115 bytes/msg | ✅ |
| Catalog split | Per-sensor messages | ✅ |
| Batching | 100KB / 50ms | ✅ |
| Idempotent producer | confluent-kafka-go | ✅ |
| Schema version header | Future-proof | ✅ |

**Result**: ~0.3 MB/day, ~110 MB/year (was ~400 MB/year). See [architecture.md](architecture.md) for resources.

## 1. Producer standards

**Required**:
- **Compression**: LZ4 (60–70% for JSON IoT).
- **Durability**: `RequiredAcks: RequireAll`, `MaxAttempts: 3`.
- **Batching**: `BatchSize: 100000` (100KB), `BatchTimeout: 50ms`.
- **Topics**: `AllowAutoTopicCreation: true`.

**Idempotency** (confluent-kafka-go): `enable.idempotence=true`, `acks=all`, `max.in.flight.requests.per.connection=5`, `retries=2147483647`. Prevents duplicates on retry.

**Alternatives**: Snappy (faster, ~50–60%); Gzip (slower, higher ratio); Zstd (best, Kafka 2.1+).

## 2. Message structure

**Required headers**: `schema_version`, `lsid`, `timestamp`, `sensor_type`, `data_structure_type`. Omit: station_id, station_id_uuid, category, product_name (use metadata lookup). Saves ~115 bytes/msg.

**Key**: `lsid:timestamp` (device + time); preserves partition ordering per device.

**Body**: JSON. Send only varying fields in data topics; constants in metadata.

**Large messages**: Split (e.g. catalog: one message per sensor type). Avoid single multi-MB messages.

## 3. Topics

**Naming**: `namespace.category[.subcategory]` (e.g. `weather.iss`, `weather.metadata.sensors`). Use dots, not hyphens/underscores.

**Data vs metadata**: Data = high frequency (FETCH_INTERVAL); metadata = low frequency, key-deduped.

**Retention**: Infinite (`KAFKA_LOG_RETENTION_MS=-1`, `KAFKA_LOG_RETENTION_BYTES=-1`) unless overridden.

## 4. Consumer standards

**Consumer group**: Descriptive name (e.g. `weatherlink-sql-data`).

**Offsets**: Commit only after successful process; do not commit on failure.

**Errors**: Persist failed messages to `orphaned_messages` (topic, partition, offset, reason, headers, body) for replay and debugging.

## 5. Monitoring and logging

**Metrics**: Publish/consume counts per topic, latency, consumer lag, orphan count.

**Log levels**: INFO (counts, skip counts); WARN (API/metadata issues); ERROR (publish/process failures); DEBUG (per-message; dev only).

## 6. Storage

**Formula**: `Annual MB ≈ sensors × messages/day × 365 × avg_size_bytes × compression_ratio / 10^6`. Example: 4 sensors, 288 msg/day, 314 bytes compressed → ~132 MB/year.

**Breakdown (approx)**: JSON body ~65%, headers ~27%, Kafka overhead ~8%. Total ~314 bytes/msg after compression.

## 7. Troubleshooting

| Issue | Check | Fix |
|-------|--------|-----|
| High storage | Compression codec in log segments | Enable LZ4, increase batch size, trim headers/body |
| Duplicates in DB | `GROUP BY tag_id, ts HAVING COUNT(*)>1` | Verify startup key scan, key format `lsid:timestamp`, unique on (tag_id, ts) |
| MessageSizeTooLarge | Message size | Split messages; avoid increasing broker max.message.bytes |

## 8. Compliance (new services)

All new Kafka services must use: LZ4 (or better), schema_version header, minimal headers, error handling (orphans), dedup strategy, metrics/logging. See [kafka-topics.md](kafka-topics.md) for schemas.

## 9. References

- **Internal**: [CLAUDE.md](../CLAUDE.md), [kafka-topics.md](kafka-topics.md), [architecture.md](architecture.md), [operations.md](operations.md)
- **External**: [Kafka Producer Configs](https://kafka.apache.org/documentation/#producerconfigs), [Confluent deployment](https://docs.confluent.io/platform/current/kafka/deployment.html), [segmentio/kafka-go](https://github.com/segmentio/kafka-go)

## Appendix: Producer snippet (segmentio/kafka-go)

```go
writer: &kafka.Writer{
    Addr: kafka.TCP(broker),
    AllowAutoTopicCreation: true,
    RequiredAcks: kafka.RequireAll,
    MaxAttempts: 3,
    Compression: kafka.Lz4,
    BatchSize: 100000,
    BatchTimeout: 50 * time.Millisecond,
}
// Headers: schema_version, lsid, timestamp, sensor_type, data_structure_type. Key: lsid:timestamp.
```
