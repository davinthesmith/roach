# Go Code Organization Standards

> **Quick reference**: [CLAUDE.md](../CLAUDE.md). This doc: full package structure and patterns.

## Principles

- **Clean architecture**: Modular packages, separation of concerns, dependency injection.
- **main.go**: &lt;100 lines; config load, wire deps, signal handling, `svc.Start(ctx)`; no business logic.
- **Context**: All long-running ops take `context.Context`; respect cancellation.
- **Errors**: Wrap with context: `fmt.Errorf("...: %w", err)`.
- **No package-level state**: Use structs with methods; no globals.
- **Naming**: Packages lowercase single word; files `snake_case`; structs/exported PascalCase; no stuttering (`service.Service` not `ServiceService`).

## Package layout

| Package | Purpose | Rules |
|---------|---------|--------|
| **config/** | Load and validate env | Return models.Config; defaults; parse duration/number; stdlib only |
| **models/** | Data structures | Pure structs, JSON/SQL tags; no logic; no external deps |
| **service/** | Business logic | DI via constructor; accept context; log here; files &lt;300 lines |
| **repository/** | DB access | Accept *sql.DB; context on ops; return models; parameterized queries; wrap errors |
| **api/** | External HTTP client | Credentials in constructor; context where possible; return models; wrap errors |
| **kafka/** | Producer/consumer | Generic; no business logic; config in constructor |
| **cache/** | In-memory cache | sync.RWMutex; Get/Set; return pointers |
| **internal/** or **util/** | Helpers | Small, focused; minimal deps; not exported outside service |

**Dependency flow**: main → config, service; service → repository, cache, models, api, kafka; repository/api/cache → models. No cycles.

## main.go pattern

```go
func main() {
    cfg := config.Load()
    // validate required, init DB/API/Kafka
    svc := service.New(cfg, deps...)
    defer svc.Close()
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() { <-sigChan; cancel() }()
    if err := svc.Start(ctx); err != nil && err != context.Canceled {
        log.Fatalf("Service error: %v", err)
    }
}
```

## Examples (snippets)

**config**: Return `models.Config`; `getEnvOrDefault(key, default)`; parse duration with `time.ParseDuration`; `log.Fatalf` on invalid.

**models**: Structs with `json` and `db` tags; group related types in one file.

**service**: `type Service struct { config models.Config; api *api.Client; repo *repository.X }`; `New(cfg, ...) *Service`; `Start(ctx context.Context) error`.

**repository**: `NewXRepository(db *sql.DB) *XRepository`; `LoadAll(ctx) ([]*models.X, error)`; `QueryContext`/`Scan`; `defer rows.Close()`; wrap errors.

**api**: `NewClient(apiKey string) *Client`; `*http.Client` with timeout; set auth header; return `[]byte` or models; wrap API errors.

**kafka**: Producer: `NewProducer(broker string)`; `WriteMessages(ctx, kafka.Message{...})`. Consumer: reader config from models.Config.

**cache**: `sync.RWMutex`; map; Get/Set with Lock/RUnlock; return pointers.

## Module and imports

**go.mod**: Simple name, e.g. `module weatherlink-sql` and `go 1.21`. Internal services: avoid long paths like `github.com/company/roach/...`.

**Imports**: Use module name: `import "weatherlink-sql/config"`, `"weatherlink-sql/models"`, `"weatherlink-sql/service"`.

## Typical service layout

```
service-name/
├── main.go
├── go.mod, go.sum, Dockerfile
├── config/config.go
├── models/types.go
├── api/          # if external API: client.go, endpoints
├── repository/   # if DB: devices.go, tags.go, records.go
├── service/      # service.go, domain.go (metadata, conditions, ...)
├── kafka/        # producer.go and/or consumer.go
├── cache/        # if needed: cache.go
└── util/ or internal/  # hash, time, topic helpers
```

See existing services: [weatherlink-kafka](../services/weatherlink-kafka/), [weatherlink-sql](../services/weatherlink-sql/) (publisher vs consumer patterns).

## Refactoring order

1. models (no deps)  
2. config (models only)  
3. repository / api (models)  
4. service (all)  
5. main.go (wire + signal)  
6. Tests, Dockerfile

## References

- [Standard Go Project Layout](https://github.com/golang-standards/project-layout)  
- [Effective Go](https://go.dev/doc/effective_go)  
- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)  
- [architecture.md](architecture.md)
