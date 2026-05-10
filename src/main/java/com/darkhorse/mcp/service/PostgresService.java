package com.darkhorse.mcp.service;

import com.darkhorse.mcp.model.PaginatedResponse;
import com.darkhorse.mcp.utils.PaginationUtils;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@ConditionalOnProperty(name = "mcp.tools.postgres.enabled", havingValue = "true", matchIfMissing = true)
public class PostgresService {

    private final JdbcTemplate jdbcTemplate;

    public PostgresService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private PaginatedResponse<Map<String, Object>> executePaginatedQuery(String sql, String cursor, Object... args) {
        int offset = PaginationUtils.decodeCursor(cursor);
        int fetchSize = PaginationUtils.PAGE_SIZE + 1; // Fetch one extra to check if there's more
        
        // Append LIMIT and OFFSET to the base query
        String paginatedSql = sql + " LIMIT " + fetchSize + " OFFSET " + offset;
        
        List<Map<String, Object>> results = jdbcTemplate.queryForList(paginatedSql, args);
        
        String nextCursor = null;
        if (results.size() > PaginationUtils.PAGE_SIZE) {
            nextCursor = PaginationUtils.encodeCursor(offset + PaginationUtils.PAGE_SIZE);
            results = results.subList(0, PaginationUtils.PAGE_SIZE); // remove the extra element
        }
        
        return new PaginatedResponse<>(results, nextCursor);
    }

    public List<Map<String, Object>> diagnoseLockContention() {
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
    }

    public PaginatedResponse<Map<String, Object>> investigateSlowQueries(int thresholdSeconds, String cursor) {
        String sql = "SELECT pid, usename, application_name, state, query, age(clock_timestamp(), query_start) AS duration " +
                "FROM pg_stat_activity " +
                "WHERE state != 'idle' AND query NOT ILIKE '%pg_stat_activity%' " +
                "AND extract(epoch from (clock_timestamp() - query_start)) > ?";
        return executePaginatedQuery(sql, cursor, thresholdSeconds);
    }

    public String analyzeTableHealth(String tableName) {
        String sql = "ANALYZE " + tableName;
        jdbcTemplate.execute(sql);
        return "Successfully recalculated query planner statistics for table " + tableName;
    }

    public List<Map<String, Object>> analyzeStorageGrowth() {
        String sql = "SELECT stat.relname AS table_name, " +
                "       pg_size_pretty(pg_total_relation_size(stat.relid)) AS total_size, " +
                "       stat.n_live_tup AS live_tuples, " +
                "       stat.n_dead_tup AS dead_tuples, " +
                "       round(stat.n_dead_tup * 100.0 / nullif(stat.n_live_tup + stat.n_dead_tup, 0), 2) AS bloat_ratio_percent " +
                "FROM pg_stat_user_tables stat " +
                "ORDER BY pg_total_relation_size(stat.relid) DESC LIMIT 15";
        return jdbcTemplate.queryForList(sql);
    }

    public List<Map<String, Object>> explainAnalyzeQuery(String query) {
        // Explain does not need pagination as it returns a structured execution plan block
        String sql = "EXPLAIN ANALYZE " + query;
        return jdbcTemplate.queryForList(sql);
    }

    public List<Map<String, Object>> detectSequentialScans() {
        String sql = "SELECT relname AS table_name, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch, " +
                "       round(seq_scan * 100.0 / nullif(seq_scan + idx_scan, 0), 2) AS seq_scan_percentage " +
                "FROM pg_stat_user_tables " +
                "WHERE seq_scan > 0 " +
                "ORDER BY seq_scan DESC LIMIT 15";
        return jdbcTemplate.queryForList(sql);
    }

    public List<Map<String, Object>> analyzeIndexEfficiency() {
        String sql = "SELECT schemaname, relname AS table_name, indexrelname AS index_name, " +
                "       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size, idx_scan " +
                "FROM pg_stat_user_indexes " +
                "WHERE idx_scan = 0 " +
                "ORDER BY pg_relation_size(indexrelid) DESC LIMIT 20";
        return jdbcTemplate.queryForList(sql);
    }

    public String applyApprovedMigration(String tableName, String indexName, String columns) {
        String sql = String.format("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)", indexName, tableName, columns);
        jdbcTemplate.execute(sql);
        return "Successfully deployed index " + indexName + " on " + tableName + "(" + columns + ") concurrently to avoid locking.";
    }
}
