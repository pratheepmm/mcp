package com.darkhorse.mcp.model;

import java.util.List;

public record PaginatedResponse<T>(
        List<T> data,
        String nextCursor
) {}
