package com.bytro.firefly;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Created by yoldeta on 2016-11-20.
 */
public class Launcher {
    public static void main(String... args) {
        System.out.println("hello");
        new Launcher().launch();
    }

    public void launch() {
        KafkaStreams streams = startStream();
        startRest(streams);
    }

    private KafkaStreams startStream() {
        return new StreamerImpl().launch();
    }

    private void startRest(KafkaStreams streams) {
        try {
            new Rester(streams).launch(8082);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
