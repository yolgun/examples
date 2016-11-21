package com.bytro.firefly;

import com.bytro.firefly.rest.RestService;
import com.bytro.firefly.stream.StreamService;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class LauncherTest {
    @Mock
    StreamService mockedStreamService;
    @Mock RestService mockedRestService;
    @InjectMocks Launcher sut;

    @Test
    public void shouldStartStream() throws Exception {
        sut.launch();
        verify(mockedStreamService, times(1)).start();
    }

    @Test
    public void shouldStartRest() throws Exception {
        sut.launch();
        verify(mockedRestService, times(1)).start(any(KafkaStreams.class), anyInt());
    }
}
