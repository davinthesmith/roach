# Go Code Organization Standards

> **For quick reference, see [AI-CONTEXT.md](AI-CONTEXT.md)**. This document provides complete code organization standards and patterns.

## Overview

All Go services in ROACH follow clean architecture principles with modular package structures, clear separation of concerns, and dependency injection patterns.

## Package Structure Standards

### Entry Point (main.go)

**Purpose**: Minimal entry point that wires dependencies and handles process lifecycle

**Rules**:
- Keep under 100 lines
- Only responsibilities: configuration loading, dependency wiring, signal handling, graceful shutdown
- No business logic
- No direct database/API/Kafka operations

**Structure**:
```go
func main() {
    cfg := config.Load()
    // Validate required fields
    // Initialize dependencies (DB, API clients)
    svc := service.New(cfg, dependencies...)
    defer svc.Close()
    
    // Setup signal handling
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigChan
        cancel()
    }()
    
    // Start service
    if err := svc.Start(ctx); err != nil && err != context.Canceled {
        log.Fatalf("Service error: %v", err)
    }
}
```

### Configuration Package (config/)

**Purpose**: Load and validate environment variables

**Rules**:
- Return structured Config object (defined in models/)
- Validate required fields
- Provide sensible defaults
- Parse complex types (durations, numbers)
- No external dependencies (only stdlib)

**Example**:
```go
func Load() models.Config {
    interval, err := time.ParseDuration(getEnvOrDefault("INTERVAL", "5m"))
    if err != nil {
        log.Fatalf("Invalid INTERVAL: %v", err)
    }
    
    return models.Config{
        RequiredField: os.Getenv("REQUIRED_FIELD"),
        OptionalField: getEnvOrDefault("OPTIONAL_FIELD", "default"),
        Interval:      interval,
    }
}
```

### Models Package (models/)

**Purpose**: Define data structures used across packages

**Rules**:
- Pure data structures (no methods except simple getters/setters)
- Use proper JSON/SQL tags
- Group related types together
- Document complex fields
- No external dependencies (except encoding tags)

**Example**:
```go
type Device struct {
    ID         int    `json:"id" db:"id"`
    LSID       int    `json:"lsid" db:"lsid"`
    SensorType int    `json:"sensor_type" db:"sensor_type"`
    Category   string `json:"category" db:"category"`
}

type Tag struct {
    ID          int     `json:"id" db:"id"`
    DeviceID    int     `json:"device_id" db:"device_id"`
    TagName     string  `json:"tag_name" db:"tag_name"`
    Unit        *string `json:"unit" db:"unit"`
}
```

### Service Package (service/)

**Purpose**: Business logic and orchestration

**Files**:
- `service.go` - Main service struct and orchestration
- `<domain>.go` - Domain-specific logic files (metadata.go, conditions.go, etc.)

**Rules**:
- Accept dependencies via constructor (dependency injection)
- All long-running operations accept `context.Context`
- Return descriptive errors with context wrapping
- Log at service layer (not in repository/api layers)
- Keep individual files under 300 lines

**Example**:
```go
type Service struct {
    config models.Config
    api    *api.Client
    repo   *repository.Repository
}

func New(cfg models.Config, apiClient *api.Client, repo *repository.Repository) *Service {
    return &Service{config: cfg, api: apiClient, repo: repo}
}

func (s *Service) Start(ctx context.Context) error {
    // Orchestration logic
}
```

### Repository Package (repository/)

**Purpose**: Database operations (if service uses a database)

**Files**: One file per entity (`devices.go`, `tags.go`, `records.go`)

**Rules**:
- Accept `*sql.DB` in constructor
- All operations accept `context.Context`
- Return domain models (from models package)
- No business logic (pure CRUD)
- Return descriptive errors with `fmt.Errorf("...: %w", err)`
- Use prepared statements or parameterized queries

**Example**:
```go
type DeviceRepository struct {
    db *sql.DB
}

func NewDeviceRepository(db *sql.DB) *DeviceRepository {
    return &DeviceRepository{db: db}
}

func (r *DeviceRepository) LoadAll(ctx context.Context) ([]*models.Device, error) {
    rows, err := r.db.QueryContext(ctx, "SELECT id, lsid FROM devices")
    if err != nil {
        return nil, fmt.Errorf("failed to query devices: %w", err)
    }
    defer rows.Close()
    
    var devices []*models.Device
    for rows.Next() {
        var d models.Device
        if err := rows.Scan(&d.ID, &d.LSID); err != nil {
            return nil, fmt.Errorf("failed to scan device: %w", err)
        }
        devices = append(devices, &d)
    }
    return devices, nil
}
```

### API Package (api/)

**Purpose**: External API client (if service calls external APIs)

**Files**:
- `client.go` - HTTP client wrapper
- `auth.go` - Authentication logic
- `<endpoint>.go` - Endpoint-specific methods

**Rules**:
- Accept credentials/config in constructor
- All operations accept `context.Context` (when possible)
- Return domain models (from models package)
- Handle errors and wrap with context
- No business logic

**Example**:
```go
type Client struct {
    apiKey     string
    httpClient *http.Client
}

func NewClient(apiKey string) *Client {
    return &Client{
        apiKey: apiKey,
        httpClient: &http.Client{Timeout: 30 * time.Second},
    }
}

func (c *Client) makeRequest(url string) ([]byte, error) {
    req, _ := http.NewRequest("GET", url, nil)
    req.Header.Set("Authorization", "Bearer "+c.apiKey)
    resp, err := c.httpClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("API failed with status %d", resp.StatusCode)
    }
    return io.ReadAll(resp.Body)
}
```

### Kafka Package (kafka/)

**Purpose**: Kafka producer/consumer utilities

**Files**: `producer.go` (producer services), `consumer.go` (consumer services)

**Rules**:
- Generic, reusable across services
- No business logic
- Accept configuration in constructor
- Handle marshalling/unmarshalling

**Example**:
```go
type Producer struct {
    writer *kafka.Writer
}

func NewProducer(broker string) *Producer {
    return &Producer{
        writer: &kafka.Writer{
            Addr:                   kafka.TCP(broker),
            AllowAutoTopicCreation: true,
        },
    }
}

func (p *Producer) Publish(ctx context.Context, topic string, data interface{}, headers map[string]string) error {
    jsonData, _ := json.Marshal(data)
    kafkaHeaders := make([]kafka.Header, 0, len(headers))
    for key, value := range headers {
        kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: key, Value: []byte(value)})
    }
    return p.writer.WriteMessages(ctx, kafka.Message{
        Topic: topic, Value: jsonData, Headers: kafkaHeaders,
    })
}
```

### Cache Package (cache/)

**Purpose**: In-memory caching (if needed)

**Rules**:
- Use `sync.RWMutex` for thread safety
- Provide Get/Set methods
- Return pointers for structs (avoid copies)
- No business logic

**Example**:
```go
type Cache struct {
    devices map[int]*models.Device
    mutex   sync.RWMutex
}

func New() *Cache {
    return &Cache{devices: make(map[int]*models.Device)}
}

func (c *Cache) GetDevice(id int) *models.Device {
    c.mutex.RLock()
    defer c.mutex.RUnlock()
    return c.devices[id]
}

func (c *Cache) SetDevice(id int, device *models.Device) {
    c.mutex.Lock()
    defer c.mutex.Unlock()
    c.devices[id] = device
}
```

### Internal Package (internal/)

**Purpose**: Internal utilities not meant for external use

**Rules**:
- Small, focused utility functions
- No external dependencies when possible
- Well-tested
- Not exported outside the service

**Example**:
```go
func CalculateHash(data []byte) string {
    hash := sha256.Sum256(data)
    return hex.EncodeToString(hash[:])
}
```

## Design Principles

### 1. Dependency Injection

All dependencies passed via constructors, never use package-level globals.

```go
// Good ✓
type Service struct {
    db *sql.DB
    api *api.Client
}

func New(db *sql.DB, apiClient *api.Client) *Service {
    return &Service{db: db, api: apiClient}
}

// Bad ✗
var globalDB *sql.DB

func (s *Service) DoSomething() {
    globalDB.Query(...) // Using global state
}
```

### 2. Context Propagation

All long-running operations accept `context.Context` for cancellation.

```go
// Good ✓
func (s *Service) Start(ctx context.Context) error {
    ticker := time.NewTicker(s.config.Interval)
    defer ticker.Stop()
    
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            s.doWork(ctx)
        }
    }
}

// Bad ✗
func (s *Service) Start() error {
    for {
        s.doWork() // No cancellation
        time.Sleep(interval)
    }
}
```

### 3. Error Wrapping

Wrap errors with context at each layer using `fmt.Errorf` with `%w`.

```go
// Good ✓
func (r *Repository) LoadDevice(ctx context.Context, id int) (*models.Device, error) {
    var d models.Device
    err := r.db.QueryRowContext(ctx, "SELECT ...", id).Scan(...)
    if err != nil {
        return nil, fmt.Errorf("failed to load device %d: %w", id, err)
    }
    return &d, nil
}

// Bad ✗
func (r *Repository) LoadDevice(ctx context.Context, id int) (*models.Device, error) {
    var d models.Device
    err := r.db.QueryRowContext(ctx, "SELECT ...", id).Scan(...)
    return &d, err // No context added
}
```

### 4. Minimal Package State

Avoid package-level variables; use structs with methods.

```go
// Good ✓
type Service struct {
    cache map[string]interface{}
    mutex sync.RWMutex
}

func (s *Service) Get(key string) interface{} {
    s.mutex.RLock()
    defer s.mutex.RUnlock()
    return s.cache[key]
}

// Bad ✗
var cache = make(map[string]interface{})
var mutex sync.RWMutex
```

### 5. Clear Naming Conventions

**Rules**:
- Package names: lowercase, single word (`config`, `service`, `repository`)
- File names: lowercase with underscores (`sensor_metadata.go`)
- Struct names: PascalCase (`Service`, `DeviceRepository`)
- Interface names: PascalCase with -er suffix (`Producer`, `Consumer`)
- Function/Method names: camelCase (unexported) or PascalCase (exported)
- No stuttering: `service.Service` not `service.ServiceService`

## Module Structure

### Module Path

Use simple module names for internal services:

```go
// go.mod - Good ✓
module weatherlink-materialize-to-sql
go 1.21

// go.mod - Bad ✗ (for internal services)
module github.com/company/roach/services/weatherlink-materialize-to-sql
go 1.21
```

### Import Paths

Within the service, use the module name:

```go
import (
    "weatherlink-materialize-to-sql/config"
    "weatherlink-materialize-to-sql/models"
    "weatherlink-materialize-to-sql/service"
)
```

## File Organization

### Typical Service Structure

```
service-name/
├── main.go              # 75-100 lines
├── go.mod, go.sum
├── Dockerfile
├── config/
│   └── config.go        # 30-50 lines
├── models/
│   └── types.go         # 50-200 lines
├── api/                 # (if external API)
│   ├── client.go        # 30-60 lines
│   ├── auth.go          # 20-40 lines
│   └── endpoints.go     # 100-200 lines
├── repository/          # (if database)
│   ├── devices.go       # 100-150 lines
│   ├── tags.go          # 100-150 lines
│   └── records.go       # 100-150 lines
├── service/
│   ├── service.go       # 100-150 lines
│   ├── metadata.go      # 150-200 lines
│   └── processing.go    # 150-200 lines
├── kafka/               # (if Kafka)
│   ├── producer.go      # 50-80 lines
│   └── consumer.go      # 50-80 lines
├── cache/               # (if caching)
│   └── cache.go         # 80-120 lines
└── internal/
    └── utils.go         # 20-50 lines
```

## Package Dependencies

**Dependency flow**:
```
main → config, service
service → repository, cache, models, api, kafka
repository → models
cache → models
api → models
kafka → none (generic)
```

**No circular dependencies allowed.**

## Real-World Examples

### weatherlink-ingest-to-kafka (Publisher Pattern)

```
weatherlink-ingest-to-kafka/
├── main.go              # Wires API client, Kafka producer, service
├── config/              # Loads WeatherLink credentials, intervals
├── models/              # API response structures
├── api/                 # WeatherLink API client
│   ├── client.go        # HTTP client wrapper
│   ├── auth.go          # HMAC-SHA256 authentication
│   └── weatherlink.go   # Endpoint methods
├── kafka/               # Kafka producer
├── service/             # Business logic
│   ├── service.go       # Orchestration and main loop
│   ├── metadata.go      # Metadata fetching with change detection
│   ├── conditions.go    # Current conditions fetching
│   └── cache.go         # Deduplication cache
└── internal/
    └── hash.go          # SHA-256 utility
```

### weatherlink-materialize-to-sql (Consumer Pattern)

```
weatherlink-materialize-to-sql/
├── main.go              # Wires DB, Kafka readers, service
├── config/              # Loads DB connection, Kafka broker
├── models/              # DB entities (Device, Tag, FieldMetadata)
├── cache/               # Thread-safe in-memory cache
├── repository/          # Database CRUD operations
│   ├── devices.go       # Device operations
│   ├── tags.go          # Tag operations with enrichment
│   ├── catalog.go       # Catalog storage
│   ├── records.go       # Record insertion
│   └── orphans.go       # Orphaned message tracking
├── service/             # Business logic processors
│   ├── materializer.go  # Service orchestration
│   ├── metadata.go      # Metadata processor
│   ├── catalog.go       # Catalog processor
│   ├── data.go          # Data processor
│   └── enrichment.go    # Tag enricher
└── kafka/
    └── consumer.go      # Kafka reader creation
```

## Benefits

**Maintainability**:
- Clear separation of concerns
- Easy to locate specific functionality
- Reduced cognitive load

**Testability**:
- Packages testable in isolation
- Easy to mock external dependencies
- Unit tests don't require infrastructure

**Scalability**:
- Clear patterns for adding features
- Easy onboarding for new developers
- Consistent structure across services

**Performance**:
- Efficient imports (only what's needed)
- Clear boundaries for optimization
- Cache and repository layers explicit

## Migration Guide

When refactoring a monolithic service to this structure:

1. Create package directories based on this guide
2. Extract models first (no dependencies)
3. Extract config second (depends only on models)
4. Extract repository/api third (depends on models)
5. Extract service fourth (depends on everything)
6. Refactor main.go last (wires everything together)
7. Update tests to match new structure
8. Update Dockerfile to copy all packages

## References

- [Standard Go Project Layout](https://github.com/golang-standards/project-layout)
- [Effective Go](https://go.dev/doc/effective_go)
- [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- [ROACH Architecture Documentation](architecture.md)
