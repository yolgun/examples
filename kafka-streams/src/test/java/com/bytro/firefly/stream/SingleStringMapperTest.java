package com.bytro.firefly.stream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by yunus on 30.11.16.
 */
public class SingleStringMapperTest {
    @Test
    public void shouldReplace() {
        String input = "unit10killed";
        SingleStringMapper sut = new SingleStringMapper(
                "unit(\\d+)killed", "group%skilled", value -> value + value);
        assertEquals("group1010killed", sut.apply(input));
    }

    @Test
    public void shouldReturnEmptyWhenInputNotMatches() {
        String input = "unit10killed1";
        SingleStringMapper sut = new SingleStringMapper(
                "unit(\\d+)killed", "group%skilled", value -> value + value);
        assertEquals("", sut.apply(input));
    }

    @Test
    public void shouldReturnEmptyWhenMapperReturnsNull() {
        String input = "unit10killed";
        SingleStringMapper sut = new SingleStringMapper(
                "unit(\\d+)killed", "group%skilled", value -> null);
        assertEquals("", sut.apply(input));
    }
}
