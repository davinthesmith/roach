# Quick Reference

## System State
- **Status**: Operational
- **Infrastructure**: Kafka, Zookeeper, Kafka UI
- **Services**: Weather (WeatherLink integration)
- **Data**: Infinite retention, ~1-5MB/day growth

## Immediate Commands
```bash
./scripts/start-all.sh          # Start system
./scripts/start-all.sh build    # Start with rebuild
./scripts/status.sh             # Check health
./scripts/logs.sh weather       # View logs
./scripts/stop-all.sh           # Stop system
```

## Key Locations
- Docs: `docs/README.md` (start here for full context)
- Go Standards: `docs/go-standards.md` (code organization)
- Config: `.env` (credentials), `docker-compose.*.yml` (services)
- Data: `./data/kafka`, `./data/zookeeper`
- Services: `services/weather/`, `services/weather-sql/`

## Network
- Kafka internal: `kafka:29092`
- Kafka external: `localhost:9092`
- Kafka UI: `http://localhost:8080`
- Network: `roach-network`

## Topics (7 total)
- Data: `weather.iss`, `weather.barometer`, `weather.indoor`, `weather.health`
- Metadata: `weather.metadata.sensors`, `weather.metadata.catalog`, `weather.metadata.station`

## Common Tasks
```bash
# Start/rebuild
./scripts/start-all.sh          # Normal start
./scripts/start-all.sh build    # Rebuild containers first

# Add service
# 1. Create services/<name>/
# 2. Add to docker-compose.yml with kafka:29092
# 3. Use topic: namespace.category.subcategory

# Debug
docker exec -it roach-weather sh
docker exec roach-kafka kafka-topics --list --bootstrap-server localhost:29092

# Clean restart
docker compose -f docker-compose.infrastructure.yml -f docker-compose.yml down -v
./scripts/start-all.sh
```

## Environment
```bash
# Required in .env
WEATHERLINK_API_KEY=<key>
WEATHERLINK_API_SECRET=<secret>
WEATHERLINK_STATION_ID=<id>

# Optional
KAFKA_BROKER=kafka:29092
FETCH_INTERVAL=5m
LOG_LEVEL=info
```

## File Structure
```
roach/
├── docker-compose.infrastructure.yml  # Kafka stack
├── docker-compose.yml                 # App services
├── .env                              # Credentials
├── scripts/*.sh                      # Operations
├── docs/*.md                         # Documentation
│   ├── README.md                     # Doc index
│   ├── go-standards.md               # Go code standards
│   └── ...
├── services/weather/                 # Weather publisher
│   ├── main.go                       # Entry point
│   ├── config/, models/, api/        # Packages
│   └── service/, kafka/, internal/
├── services/weather-sql/             # Weather materializer
│   ├── main.go                       # Entry point
│   ├── config/, models/, cache/      # Packages
│   └── repository/, service/, kafka/
└── data/                             # Persistent storage
```

## Troubleshooting Quick Fixes
- Won't start: Check Docker running, `docker ps`
- Connection refused: Wait 30-60s for Kafka health check
- No data: Verify `.env` credentials, `./scripts/logs.sh weather`
- Out of disk: `du -sh data/`, delete old data
- Clean slate: `down -v`, `rm -rf data/`, `start-all.sh`

## Docs Navigation
Read in order for complete understanding:
1. `docs/architecture.md` - System design
2. `docs/configuration.md` - Settings
3. `docs/operations.md` - Day-to-day use
4. `docs/kafka-topics.md` - Data schemas
5. `docs/api-reference.md` - Commands
6. `docs/troubleshooting.md` - Problem solving
7. `docs/weather-service.md` - Service details
