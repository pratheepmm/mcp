package com.darkhorse.mcp.tools;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Map;

@Service
public class PostgresTools {

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public PostgresTools(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Diagnose active lock contention by mapping blocked queries to the exact processes and users blocking them.")
    public Mono<List<Map<String, Object>>> diagnoseLockContention() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT blocked_locks.pid AS blocked_pid, " +
                    "       blocked_activity.usename AS blocked_user, " +
                    "       blocking_locks.pid AS blocking_pid, " +
                    "       blocking_activity.usename AS blocking_user, " +
                    "       blocked_activity.query AS blocked_query, " +
                    "       blocking_activity.query AS blocking_query " +
                    "FROM pg_catalog.pg_locks blocked_locks " +
                    "JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid " +
                    "JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype " +
                    "     AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE " +
                    "     AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation " +
                    "     AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page " +
                    "     AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple " +
                    "     AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid " +
                    "     AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid " +
                    "     AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid " +
                    "     AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid " +
                    "     AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid " +
                    "     AND blocking_locks.pid != blocked_locks.pid " +
                    "JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid " +
                    "WHERE NOT blocked_locks.GRANTED";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Investigate queries running longer than the specified threshold (in seconds) to identify active latency spikes.")
    public Mono<List<Map<String, Object>>> investigateSlowQueries(int thresholdSeconds) {
        return Mono.fromCallable(() -> {
            String sql = "SELECT pid, usename, application_name, state, query, age(clock_timestamp(), query_start) AS duration " +
                    "FROM pg_stat_activity " +
                    "WHERE state != 'idle' AND query NOT ILIKE '%pg_stat_activity%' " +
                    "AND extract(epoch from (clock_timestamp() - query_start)) > ?";
            return jdbcTemplate.queryForList(sql, thresholdSeconds);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: false] Analyze table health by executing an ANALYZE command to recalculate statistics for the query planner. Use when execution plans suddenly degrade.",
             annotations = @McpTool.McpAnnotations(destructiveHint = true))
    public Mono<String> analyzeTableHealth(String tableName) {
        return Mono.fromCallable(() -> {
            String sql = "ANALYZE " + tableName;
            jdbcTemplate.execute(sql);
            return "Successfully recalculated query planner statistics for table " + tableName;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Analyze storage growth, table sizes, and dead tuple bloat to detect tables requiring vacuuming or partitioning.")
    public Mono<List<Map<String, Object>>> analyzeStorageGrowth() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT stat.relname AS table_name, " +
                    "       pg_size_pretty(pg_total_relation_size(stat.relid)) AS total_size, " +
                    "       stat.n_live_tup AS live_tuples, " +
                    "       stat.n_dead_tup AS dead_tuples, " +
                    "       round(stat.n_dead_tup * 100.0 / nullif(stat.n_live_tup + stat.n_dead_tup, 0), 2) AS bloat_ratio_percent " +
                    "FROM pg_stat_user_tables stat " +
                    "ORDER BY pg_total_relation_size(stat.relid) DESC LIMIT 15";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Generate a detailed execution plan for a query to diagnose missing indexes or poor join strategies. (Warning: Runs the query to analyze it)")
    public Mono<List<Map<String, Object>>> explainAnalyzeQuery(String query) {
        return Mono.fromCallable(() -> {
            String sql = "EXPLAIN ANALYZE " + query;
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Detect sequential scans across all tables. High seq scans vs index scans strongly indicates a missing index.")
    public Mono<List<Map<String, Object>>> detectSequentialScans() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT relname AS table_name, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, " +
                    "       round(seq_scan * 100.0 / nullif(seq_scan + idx_scan, 0), 2) AS seq_scan_percentage " +
                    "FROM pg_stat_user_tables " +
                    "WHERE seq_scan > 0 " +
                    "ORDER BY seq_scan DESC LIMIT 15";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: LOW, Read-Only: true] Audit index efficiency by identifying unused indexes that are consuming write IO and storage without providing read benefits.")
    public Mono<List<Map<String, Object>>> analyzeIndexEfficiency() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT schemaname, relname AS table_name, indexrelname AS index_name, " +
                    "       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size, idx_scan " +
                    "FROM pg_stat_user_indexes " +
                    "WHERE idx_scan = 0 " +
                    "ORDER BY pg_relation_size(indexrelid) DESC LIMIT 20";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "[Risk: MEDIUM, Read-Only: false] Propose and apply an index creation to fix query latency. Use this after detecting seq scans or analyzing execution plans.",
             annotations = @McpTool.McpAnnotations(destructiveHint = true))
    public Mono<String> applyApprovedMigration(String tableName, String indexName, String columns) {
        return Mono.fromCallable(() -> {
            String sql = String.format("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)", indexName, tableName, columns);
            jdbcTemplate.execute(sql);
            return "Successfully deployed index " + indexName + " on " + tableName + "(" + columns + ") concurrently to avoid locking.";
        }).subscribeOn(Schedulers.boundedElastic());
    }
}
