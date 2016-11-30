package com.bytro.firefly.stream;

import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by yunus on 30.11.16.
 */
public class MultiStringMapperTest {
    private final static String VALID_ANSWER = "VALID_ANSWER";

    @Test
    public void shouldReturnTwoValidAnswers() throws Exception {
        MultiStringMapper sut = new MultiStringMapper();
        SingleStringMapper identityMapper = mock(SingleStringMapper.class);
        when(identityMapper.apply(anyString())).thenReturn(VALID_ANSWER);
        SingleStringMapper nullMapper = mock(SingleStringMapper.class);
        sut.addSingleMapper(identityMapper);
        sut.addSingleMapper(identityMapper);
        sut.addSingleMapper(nullMapper);

        assertThat(sut.apply("ANY"), containsInAnyOrder(VALID_ANSWER, VALID_ANSWER));
    }

}