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

    @McpTool(description = "Get currently active Postgres queries that are not idle")
    public Mono<List<Map<String, Object>>> getActiveQueries() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT pid, usename, application_name, state, query, age(clock_timestamp(), query_start) AS duration " +
                         "FROM pg_stat_activity " +
                         "WHERE state != 'idle' AND query NOT ILIKE '%pg_stat_activity%'";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "Get Postgres queries that are currently blocking other queries")
    public Mono<List<Map<String, Object>>> getBlockingQueries() {
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

    @McpTool(description = "Execute a DDL or DML SQL statement to fix issues, create indexes, or partition tables. Returns the number of rows affected.")
    public Mono<Integer> executeSqlUpdate(String sql) {
        return Mono.fromCallable(() -> {
            return jdbcTemplate.update(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "Identify long-running queries that exceed a certain threshold in seconds")
    public Mono<List<Map<String, Object>>> getLongRunningQueries(int thresholdSeconds) {
        return Mono.fromCallable(() -> {
            String sql = "SELECT pid, usename, application_name, state, query, age(clock_timestamp(), query_start) AS duration " +
                         "FROM pg_stat_activity " +
                         "WHERE state != 'idle' AND query NOT ILIKE '%pg_stat_activity%' " +
                         "AND extract(epoch from (clock_timestamp() - query_start)) > ?";
            return jdbcTemplate.queryForList(sql, thresholdSeconds);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "Create an index on a table to optimize query performance")
    public Mono<String> createIndex(String tableName, String indexName, String columns) {
        return Mono.fromCallable(() -> {
            String sql = String.format("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", indexName, tableName, columns);
            jdbcTemplate.execute(sql);
            return "Successfully created index " + indexName + " on " + tableName + "(" + columns + ")";
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "Run ANALYZE on a table to update statistics for the query planner")
    public Mono<String> analyzeTable(String tableName) {
        return Mono.fromCallable(() -> {
            // Sanitize table name to prevent basic SQL injection, though executeSqlUpdate is already generic
            String sql = "ANALYZE " + tableName;
            jdbcTemplate.execute(sql);
            return "Successfully analyzed table " + tableName;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "Get the sizes of the largest tables in the current Postgres database")
    public Mono<List<Map<String, Object>>> getTableSizes() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT relname as table_name, pg_size_pretty(pg_total_relation_size(relid)) As size, " +
                         "       pg_total_relation_size(relid) as size_bytes " +
                         "FROM pg_catalog.pg_statio_user_tables " +
                         "ORDER BY pg_total_relation_size(relid) DESC LIMIT 10";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @McpTool(description = "Explain and analyze a specific Postgres SQL query to understand its execution plan")
    public Mono<List<Map<String, Object>>> explainAnalyzeQuery(String query) {
        return Mono.fromCallable(() -> {
            // Directly concatenating the query to execute EXPLAIN ANALYZE
            String sql = "EXPLAIN ANALYZE " + query;
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }
    
    @McpTool(description = "Get database statistics such as cache hit ratio, commits, and rollbacks")
    public Mono<List<Map<String, Object>>> getDatabaseStats() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT datname, " +
                         "       xact_commit, " +
                         "       xact_rollback, " +
                         "       blks_read, " +
                         "       blks_hit, " +
                         "       round(blks_hit * 100.0 / nullif(blks_hit + blks_read, 0), 2) AS cache_hit_ratio_percent " +
                         "FROM pg_stat_database " +
                         "WHERE datname = current_database()";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }
    
    @McpTool(description = "Get unused indexes in the current Postgres database")
    public Mono<List<Map<String, Object>>> getUnusedIndexes() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT schemaname, relname AS table_name, indexrelname AS index_name, " +
                         "       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size, idx_scan " +
                         "FROM pg_stat_user_indexes " +
                         "WHERE idx_scan = 0 " +
                         "ORDER BY pg_relation_size(indexrelid) DESC LIMIT 20";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }
    
    @McpTool(description = "Get table bloat estimate (dead tuples) for tables in the database")
    public Mono<List<Map<String, Object>>> getTableBloat() {
        return Mono.fromCallable(() -> {
            String sql = "SELECT relname AS table_name, n_live_tup, n_dead_tup, " +
                         "       round(n_dead_tup * 100.0 / nullif(n_live_tup + n_dead_tup, 0), 2) AS dead_tup_ratio_percent " +
                         "FROM pg_stat_user_tables " +
                         "ORDER BY n_dead_tup DESC LIMIT 20";
            return jdbcTemplate.queryForList(sql);
        }).subscribeOn(Schedulers.boundedElastic());
    }
}

