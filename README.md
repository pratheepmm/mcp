# Postgres & Kafka MCP Diagnostics Server

This is an advanced Model Context Protocol (MCP) server built with Spring Boot and Spring AI. It provides production-grade database and message broker debugging capabilities to AI agents. It uses MCP Tool Consolidation to provide high-level, unified interfaces to the LLM.

## Prerequisites

- Java 26
- Docker and Docker Compose (for the database and Kafka cluster)

## Getting Started

### 1. Start the Infrastructure

Use Docker Compose to start the local PostgreSQL and Apache Kafka instances:

```bash
docker-compose up -d
```

This will start:
- A Postgres instance on `localhost:5432` (Username: `postgres`, Password: `password`)
- A KRaft-mode Kafka broker on `localhost:9092`

### 2. Build the Server

Build the executable JAR:

```bash
./gradlew bootJar
```

### 3. Configure Your MCP Client

This server is currently configured to run in **SSE (Server-Sent Events)** mode on port `8080`.

To use this with an MCP client (like Claude Desktop), add the following to your configuration file:

```json
{
  "mcpServers": {
    "infra-debugging": {
      "url": "http://localhost:8080/mcp/sse",
      "env": {
        "MCP_TOOLS_POSTGRES_ENABLED": "true",
        "MCP_TOOLS_KAFKA_ENABLED": "true"
      }
    }
  }
}
```

> **Note**: For SSE mode, you must first start the server manually (e.g., `./gradlew bootRun`) before the client can connect.

#### Enabling/Disabling Toolsets
You can load only the tools you need by setting the following environment variables (both default to `true`):
- `MCP_TOOLS_POSTGRES_ENABLED=false` (Disables all Postgres tools)
- `MCP_TOOLS_KAFKA_ENABLED=false` (Disables all Kafka tools)

## Available Tools (Consolidated)

The server employs the **MCP Tool Consolidation Strategy** to present a simple interface to the AI while supporting vast capabilities underneath.

### Postgres Tools
1. **`postgresDiagnostics`** (Read-Only)
   - `DIAGNOSE_LOCK_CONTENTION`: Maps blocked queries to the exact processes blocking them.
   - `INVESTIGATE_SLOW_QUERIES`: Identifies active latency spikes.
   - `ANALYZE_STORAGE_GROWTH`: Detects tables requiring vacuuming/partitioning.
   - `DETECT_SEQUENTIAL_SCANS`: Finds tables suffering from high sequential scans.
   - `ANALYZE_INDEX_EFFICIENCY`: Audits unused indexes consuming write IO.
   - `EXPLAIN_ANALYZE_QUERY`: Generates execution plans.
2. **`postgresMigration`** (Requires Explicit Client Approval)
   - `ANALYZE_TABLE_HEALTH`: Recalculates query planner statistics.
   - `APPLY_APPROVED_MIGRATION`: Proposes and applies index creations safely (concurrently).

### Kafka Tools
1. **`kafkaDiagnostics`** (Read-Only)
   - `DISCOVER_TOPICS`: Lists topics.
   - `ANALYZE_TOPIC_HEALTH`: Checks partition counts, leaders, and under-replicated partitions.
   - `AUDIT_TOPIC_CONFIG`: Reviews topic configurations (retention, etc.).
   - `ANALYZE_CONSUMER_LAG`: Calculates consumer lag offsets.
   - `INSPECT_PARTITION_OFFSETS`: Maps throughput distribution.
   - `DETECT_PARTITION_SKEW`: Diagnoses hot-spotting and poor partition keys.
2. **`kafkaMigration`** (Requires Explicit Client Approval)
   - `APPLY_PARTITION_SCALING`: Dynamically scales partition counts for high-traffic topics.

## Changelog

**v1.2.0 - Tool Consolidation & Env Flags**
- **Refactoring:** Consolidated 15 specific tools into 4 "Fat Tools" (`postgresDiagnostics`, `postgresMigration`, `kafkaDiagnostics`, `kafkaMigration`) using Enum actions to reduce LLM context overhead.
- **Service Isolation:** Extracted all raw JDBC and Kafka Admin logic into `PostgresService` and `KafkaService`.
- **Environment Toggles:** Introduced `@ConditionalOnProperty` to selectively load Postgres or Kafka tools via `MCP_TOOLS_POSTGRES_ENABLED` and `MCP_TOOLS_KAFKA_ENABLED`.

**v1.1.0 - Operational Intelligence Upgrade**
- Shifted from generic API wrappers to intelligent operational workflows.
- Removed dangerous `executeSqlUpdate` in favor of scoped migration tools.
- Enforced client-side execution approval by embedding `@McpAnnotations(destructiveHint = true)` on all non-read-only tools.
- Renamed tools to be task-oriented (e.g., `analyzeTopicHealth`, `diagnoseLockContention`).

**v1.0.0 - Initial Reactive Release**
- Transitioned Spring AI MCP Server to WebFlux ASYNC mode.
- Added Kafka KRaft broker alongside Postgres.
- Implemented core diagnostic methods wrapped in `Mono.fromCallable()`.
