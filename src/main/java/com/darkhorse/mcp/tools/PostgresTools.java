package com.darkhorse.mcp.tools;

import org.springframework.ai.mcp.annotation.McpTool;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Autowired;

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
    public List<Map<String, Object>> getActiveQueries() {
        String sql = "SELECT pid, usename, application_name, state, query, age(clock_timestamp(), query_start) AS duration " +
                     "FROM pg_stat_activity " +
                     "WHERE state != 'idle' AND query NOT ILIKE '%pg_stat_activity%'";
        return jdbcTemplate.queryForList(sql);
    }

    @McpTool(description = "Get Postgres queries that are currently blocking other queries")
    public List<Map<String, Object>> getBlockingQueries() {
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

    @McpTool(description = "Terminate a specific Postgres backend process by PID")
    public String terminateBackend(int pid) {
        String sql = "SELECT pg_terminate_backend(?)";
        Boolean terminated = jdbcTemplate.queryForObject(sql, Boolean.class, pid);
        return Boolean.TRUE.equals(terminated) ? "Successfully terminated process " + pid : "Failed to terminate process " + pid;
    }

    @McpTool(description = "Get the sizes of the largest tables in the current Postgres database")
    public List<Map<String, Object>> getTableSizes() {
        String sql = "SELECT relname as table_name, pg_size_pretty(pg_total_relation_size(relid)) As size, " +
                     "       pg_total_relation_size(relid) as size_bytes " +
                     "FROM pg_catalog.pg_statio_user_tables " +
                     "ORDER BY pg_total_relation_size(relid) DESC LIMIT 10";
        return jdbcTemplate.queryForList(sql);
    }

    @McpTool(description = "Explain and analyze a specific Postgres SQL query to understand its execution plan")
    public List<Map<String, Object>> explainAnalyzeQuery(String query) {
        // Directly concatenating the query to execute EXPLAIN ANALYZE
        String sql = "EXPLAIN ANALYZE " + query;
        return jdbcTemplate.queryForList(sql);
    }
}

