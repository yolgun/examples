package com.bytro.firefly.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by yunus on 29.11.16.
 */
public class UnitToGroupMapper implements Function<String,String> {
    private static final UnitToGroupMapper instance = new UnitToGroupMapper();
    private final Map<String,String> mapper;
    private static final int SCORE_COUNT = 10;

    private UnitToGroupMapper() {
        mapper = new HashMap<>();
        for (int i = 0; i < SCORE_COUNT * 2; i++) {
            mapper.put("" + i, "" + (i / 3));
        }
    }

    public String apply(String input) {
        return mapper.get(input);
    }

    public static UnitToGroupMapper getInstance() {
        return instance;
    }
}
