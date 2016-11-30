package com.bytro.firefly.stream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by yunus on 30.11.16.
 */
public class MultiStringMapper implements Function<String, Iterable<String>> {
    Collection<SingleStringMapper> mappers = new ArrayList<>();

    public MultiStringMapper addSingleMapper(SingleStringMapper mapper) {
        mappers.add(mapper);
        return this;
    }

    @Override
    public Collection<String> apply(String s) {
        return mappers.stream()
                .map(mapper -> mapper.apply(s))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
