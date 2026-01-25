# ROACH Documentation

## Documentation Guidelines

**IMPORTANT**
**IMPORTANT**
When working with this project:
1. **DO NOT generate additional .md files in the project root directory** - New documentation belongs in `docs/` directory. If new features are added that impact user or developer experience, update documentation in `docs/`. Keep it terse and optimized for AI Agents.
2. **NO historical information outside of CHANGELOG.md** - All changes, fixes, checklists, summaries, solution details, and updates belong in CHANGELOG.md only
3. **DO keep AI-CONTEXT.md updated**: - Ensure any pertinent changes are included. (optimized)

**For AI Agents**: Start with [AI-CONTEXT.md](AI-CONTEXT.md) - single file with 80% of what you need (740 lines).

## Documentation Structure

### Quick Start (Read First)
- **[AI-CONTEXT.md](AI-CONTEXT.md)** - **START HERE** - Consolidated context covering system overview, architecture, configuration, operations, services, topics, database, troubleshooting, and code standards

### Deep Dive References
- **[architecture.md](architecture.md)** - Detailed component specifications, network details, resource metrics
- **[operations.md](operations.md)** - Advanced operations, maintenance procedures, database operations
- **[troubleshooting.md](troubleshooting.md)** - Comprehensive problem solving, all known issues and solutions
- **[go-standards.md](go-standards.md)** - Complete Go code organization standards and patterns
- **[kafka-standards.md](kafka-standards.md)** - Kafka best practices, optimization guide, storage analysis
- **[kafka-topics.md](kafka-topics.md)** - Full topic schemas, all fields, message formats
- **[migrations.md](migrations.md)** - Database migration framework details

### Scripts Documentation
- **[scripts/README.md](../scripts/README.md)** - **Complete script documentation** - All operational scripts with every option, examples, and workflows

## Usage Guide

**For general tasks**: Read [AI-CONTEXT.md](AI-CONTEXT.md) (1 file, covers 80% of needs)

**For script usage**: See [scripts/README.md](../scripts/README.md) (complete reference for all operational scripts)

**For specific deep dives**:
- System design questions → [architecture.md](architecture.md)
- Operational issues → [operations.md](operations.md)
- Problems/errors → [troubleshooting.md](troubleshooting.md)
- Code organization → [go-standards.md](go-standards.md)
- Kafka best practices → [kafka-standards.md](kafka-standards.md)
- Topic schemas → [kafka-topics.md](kafka-topics.md)