package com.darkhorse.mcp.tools;

import com.darkhorse.mcp.service.PostgresService;
import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class PostgresTools {

    private final PostgresService postgresService;

    public PostgresTools(PostgresService postgresService) {
        this.postgresService = postgresService;
    }

    public enum PostgresDiagnosticAction {
        DIAGNOSE_LOCK_CONTENTION,
        INVESTIGATE_SLOW_QUERIES,
        ANALYZE_STORAGE_GROWTH,
        DETECT_SEQUENTIAL_SCANS,
        ANALYZE_INDEX_EFFICIENCY,
        EXPLAIN_ANALYZE_QUERY
    }

    public record PostgresDiagnosticRequest(
            PostgresDiagnosticAction action,
            Integer thresholdSeconds,
            String query
    ) {}

    @McpTool(description = "[Risk: LOW, Read-Only: true] Unified Postgres diagnostic tool. Provides lock contention, slow queries, storage bloat, sequential scans, index efficiency, and execution plans.")
    public Mono<Object> postgresDiagnostics(PostgresDiagnosticRequest request) {
        return Mono.<Object>fromCallable(() -> {
            return switch (request.action()) {
                case DIAGNOSE_LOCK_CONTENTION -> postgresService.diagnoseLockContention();
                case INVESTIGATE_SLOW_QUERIES -> postgresService.investigateSlowQueries(
                        request.thresholdSeconds() != null ? request.thresholdSeconds() : 60);
                case ANALYZE_STORAGE_GROWTH -> postgresService.analyzeStorageGrowth();
                case DETECT_SEQUENTIAL_SCANS -> postgresService.detectSequentialScans();
                case ANALYZE_INDEX_EFFICIENCY -> postgresService.analyzeIndexEfficiency();
                case EXPLAIN_ANALYZE_QUERY -> postgresService.explainAnalyzeQuery(request.query());
            };
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public enum PostgresMigrationAction {
        ANALYZE_TABLE_HEALTH,
        APPLY_APPROVED_MIGRATION
    }

    public record PostgresMigrationRequest(
            PostgresMigrationAction action,
            String tableName,
            String indexName,
            String columns
    ) {}

    @McpTool(description = "[Risk: MEDIUM, Read-Only: false] Unified Postgres migration tool. Propose and apply an index creation concurrently, or recalculate table statistics to fix execution plans.",
             annotations = @McpTool.McpAnnotations(destructiveHint = true))
    public Mono<String> postgresMigration(PostgresMigrationRequest request) {
        return Mono.fromCallable(() -> {
            return switch (request.action()) {
                case ANALYZE_TABLE_HEALTH -> postgresService.analyzeTableHealth(request.tableName());
                case APPLY_APPROVED_MIGRATION -> postgresService.applyApprovedMigration(
                        request.tableName(), request.indexName(), request.columns());
            };
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
