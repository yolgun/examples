package com.bytro.firefly;

import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScoreValue;
import io.confluent.examples.streams.utils.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * Created by yunus on 27.11.16.
 */
public class AwardIT {
    private static final int USER_COUNT = 10;
    private static final int GAME_COUNT = 10;
    private static final int SCORE_COUNT = 10;
    private static final int EVENT_COUNT = 10;
    private static final String INTPUT_TOPIC = "firefly10-read";

    public KafkaProducer<User,UserGameScoreValue> givenProducer() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.101.10:9092");
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        producerConfig.put("schema.registry.url", "http://192.168.101.10:8081");
        return new KafkaProducer<>(producerConfig);
    }

    @Test
    public void testAward() {
        KafkaProducer<User,UserGameScoreValue> producer = givenProducer();
        whenSendMessages(producer);
    }

    private void whenSendMessages(KafkaProducer<User, UserGameScoreValue> producer) {
        ArrayList<Future> futures = new ArrayList<>();
        IntStream.range(0, USER_COUNT).forEach(userID ->
                IntStream.range(0, GAME_COUNT).forEach(gameID ->
                        IntStream.range(0, SCORE_COUNT).mapToObj(i -> "score-" + i).forEach(scoreID ->
                                IntStream.range(0, userID + 1).forEach( eventID ->
                                        futures.add(sendMessage(producer, userID, gameID, scoreID))
                                )
                        )
                )
        );
        futures.forEach(f -> {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });
    }

    private Future sendMessage(KafkaProducer<User, UserGameScoreValue> producer, int userID, int gameID, String scoreID) {
        ProducerRecord record = new ProducerRecord<>(
                INTPUT_TOPIC,
                new User(userID),
                new UserGameScoreValue(userID, gameID, scoreID, 1)
        );
        System.out.println(record);
//        return null;
        return producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println(exception.getMessage());
                }
            }
        });
    }
}
