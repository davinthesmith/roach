# ROACH Documentation

## Purpose
This documentation is optimized for AI agent consumption to understand and work with the ROACH project.

## Structure

### Start Here
- **[quick-reference.md](quick-reference.md)** - **START HERE** - Essential info in <1 page

### Core Documentation
- **[architecture.md](architecture.md)** - System architecture, data flow, components
- **[configuration.md](configuration.md)** - Environment variables, settings, customization
- **[operations.md](operations.md)** - Running, monitoring, maintaining the system

### Reference
- **[kafka-topics.md](kafka-topics.md)** - Topic naming, schemas, message structures
- **[api-reference.md](api-reference.md)** - Command reference, API endpoints
- **[troubleshooting.md](troubleshooting.md)** - Common issues and solutions

### Services
- **[weather-service.md](weather-service.md)** - Weather publisher and SQL materializer services

### Database
- **PostgreSQL** - Time-series data storage with Device/Tag/Record hierarchy

## How to Use This Documentation

### For AI Agents - Quick Start
**Read `quick-reference.md` first** - Contains all essential info in one page.

For complete context, read in order:
1. `quick-reference.md` - Essential overview
2. `architecture.md` - Understand the system
3. `configuration.md` - Know what can be configured
4. `operations.md` - Learn how to operate it
5. `kafka-topics.md` - Understand data organization
6. Service-specific docs as needed

### For Quick Tasks
- **Start/stop system**: See `quick-reference.md` or `operations.md`
- **Add new service**: See `architecture.md` → Extension Points
- **Debug issues**: See `troubleshooting.md`
- **Configure settings**: See `configuration.md`

### For Development
- **Service structure**: See `architecture.md` → Services
- **Kafka integration**: See `kafka-topics.md`
- **API reference**: See `api-reference.md`

## Documentation Principles

1. **Concise**: Only essential information, no verbosity
2. **Structured**: Hierarchical organization for easy parsing
3. **Actionable**: Commands and code examples included
4. **Current**: Reflects actual implementation, not aspirational
5. **Context-rich**: Enough detail for autonomous AI agents
6. **No History**: Historical information belongs in CHANGELOG.md only
