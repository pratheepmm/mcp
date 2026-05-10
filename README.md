# Postgres Debugging MCP Server

This is a Model Context Protocol (MCP) server built with Spring Boot and Spring AI to provide PostgreSQL debugging tools to AI agents.

## Prerequisites

- Java 26
- Docker and Docker Compose (for the database)

## Getting Started

### 1. Start the Postgres Database

Use Docker Compose to start the local PostgreSQL instance:

```bash
docker-compose up -d
```

This will start a Postgres instance on `localhost:5432` with:
- **Username**: `postgres`
- **Password**: `password`
- **Database**: `postgres`

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
    "postgres-debugging": {
      "url": "http://localhost:8080/mcp/sse"
    }
  }
}
```

> **Note**: For SSE mode, you must first start the server manually (e.g., `./gradlew bootRun`) before the client can connect.

#### Alternative: STDIO Mode
If you prefer the client to manage starting the server, switch back to STDIO mode in `application.properties` and use the following config:

```json
{
  "mcpServers": {
    "postgres-debugging": {
      "command": "java",
      "args": [
        "-jar",
        "/Users/Pratheep/Downloads/projects/mcp/build/libs/mcp-0.0.1-SNAPSHOT.jar"
      ]
    }
  }
}
```

## Available Tools

- `getActiveQueries`: Get currently active Postgres queries that are not idle.
- `getBlockingQueries`: Get Postgres queries that are currently blocking other queries.
- `terminateBackend`: Terminate a specific Postgres backend process by PID.
- `getTableSizes`: Get the sizes of the largest tables in the current Postgres database.
- `explainAnalyzeQuery`: Explain and analyze a specific Postgres SQL query to understand its execution plan.

## Configuration Details

- **STDIO Mode**: The server is configured to communicate via standard I/O (`spring.ai.mcp.server.stdio=true`).
- **Logging**: All logs are routed to `mcp-server.log` to avoid interfering with the JSON-RPC communication on stdout.
