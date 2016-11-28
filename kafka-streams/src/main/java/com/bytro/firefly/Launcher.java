package com.bytro.firefly;

import com.bytro.firefly.rest.RestService;
import com.bytro.firefly.stream.StreamService;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main launcher class. Responsible from lifecycle of streaming and rest services
 * <p>
 * Created by yoldeta on 2016-11-20.
 */
public class Launcher {
    private static final int REST_PORT = 8083;
    private static final Logger logger = LoggerFactory.getLogger(Launcher.class);

    private StreamService streamService;
    private RestService restService;

    public Launcher() {
        this(new StreamService(), new RestService());
    }

    public Launcher(StreamService streamService, RestService restService) {
        this.streamService = streamService;
        this.restService = restService;
    }

    public static void main(String... args) {
        logger.info("Starting...");
        try {
            new Launcher().launch();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void launch() {
        startStream();
        startRest();
        addShutdownHooks();
    }

    private void startStream() {
        streamService.start();
    }

    private void startRest() {
        KafkaStreams stream = streamService.getStreams();
        restService.start(stream, REST_PORT);
    }

    private void addShutdownHooks() {
        Runtime.getRuntime()
                .addShutdownHook(new Thread(this::shutdown));
    }

    private void shutdown() {
        logger.info("Shutting down...");
        streamService.stop();
        restService.stop();
    }
}
