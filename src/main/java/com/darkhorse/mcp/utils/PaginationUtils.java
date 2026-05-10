package com.darkhorse.mcp.utils;

import com.darkhorse.mcp.model.PaginatedResponse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

public class PaginationUtils {

    public static final int PAGE_SIZE = 20;

    public static int decodeCursor(String cursor) {
        if (cursor == null || cursor.isBlank()) return 0;
        try {
            return Integer.parseInt(new String(Base64.getDecoder().decode(cursor), StandardCharsets.UTF_8));
        } catch (Exception e) {
            return 0;
        }
    }

    public static String encodeCursor(int nextOffset) {
        return Base64.getEncoder().encodeToString(String.valueOf(nextOffset).getBytes(StandardCharsets.UTF_8));
    }

    public static <T> PaginatedResponse<T> paginateList(List<T> fullList, String cursor) {
        int offset = decodeCursor(cursor);
        int total = fullList.size();

        if (offset >= total) {
            return new PaginatedResponse<>(Collections.emptyList(), null);
        }

        int end = Math.min(offset + PAGE_SIZE, total);
        List<T> subList = fullList.subList(offset, end);

        String nextCursor = null;
        if (end < total) {
            nextCursor = encodeCursor(end);
        }

        return new PaginatedResponse<>(subList, nextCursor);
    }
}
