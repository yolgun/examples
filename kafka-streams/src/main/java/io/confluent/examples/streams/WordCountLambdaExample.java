///**
// * Copyright 2016 Confluent Inc.
// * <p>
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// * in compliance with the License. You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software distributed under the License
// * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// * or implied. See the License for the specific language governing permissions and limitations under
// * the License.
// */
//package io.confluent.examples.streams;
//
//import com.bytro.firefly.avro.Award;
//import com.bytro.firefly.avro.ScoreValue;
//import com.bytro.firefly.avro.User;
//import com.bytro.firefly.avro.UserGameScore;
//import com.bytro.firefly.avro.UserGameScoreValue;
//import com.bytro.firefly.avro.UserScore;
//import io.confluent.examples.streams.utils.SpecificAvroSerde;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.KeyValue;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.kafka.streams.kstream.KStreamBuilder;
//
//import java.util.Collections;
//import java.util.Properties;
//
///**
// * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program that
// * computes a simple word occurrence histogram from an input text. This example uses lambda
// * expressions and thus works with Java 8+ only.
// * <p>
// * In this example, the input stream reads from a topic named "TextLinesTopic", where the values of
// * messages represent lines of text; and the histogram output is written to topic
// * "WordsWithCountsTopic", where each record is an updated count of a single word, i.e. {@code word (String) -> currentCount (Long)}.
// * <p>
// * Note: Before running this example you must 1) create the source topic (e.g. via {@code kafka-topics --create ...}),
// * then 2) start this example and 3) write some data to the source topic (e.g. via {@code kafka-console-producer}).
// * Otherwise you won't see any data arriving in the output topic.
// * <p>
// * <br>
// * HOW TO RUN THIS EXAMPLE
// * <p>
// * 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
// * <p>
// * 2) Create the input and output topics used by this example.
// * <pre>
// * {@code
// * $ bin/kafka-topics --create --topic TextLinesTopic \
// *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
// * $ bin/kafka-topics --create --topic WordsWithCountsTopic \
// *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
// * }</pre>
// * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
// * <p>
// * 3) Start this example application either in your IDE or on the command line.
// * <p>
// * If via the command line please refer to <a href='https://github.com/confluentinc/examples/tree/master/kafka-streams#packaging-and-running'>Packaging</a>.
// * Once packaged you can then run:
// * <pre>
// * {@code
// * $ java -cp target/streams-examples-3.1.0-standalone.jar io.confluent.examples.streams.WordCountLambdaExample
// * }</pre>
// * 4) Write some input data to the source topics (e.g. via {@code kafka-console-producer}). The already
// * running example application (step 3) will automatically process this input data and write the
// * results to the output topic.
// * <pre>
// * {@code
// * # Start the console producer. You can then enter input data by writing some line of text, followed by ENTER:
// * #
// * #   hello kafka streams<ENTER>
// * #   all streams lead to kafka<ENTER>
// * #   join kafka summit<ENTER>
// * #
// * # Every line you enter will become the value of a single Kafka message.
// * $ bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic
// * }</pre>
// * 5) Inspect the resulting data in the output topics, e.g. via {@code kafka-console-consumer}.
// * <pre>
// * {@code
// * $ bin/kafka-console-consumer --topic WordsWithCountsTopic --from-beginning \
// *                              --zookeeper localhost:2181 \
// *                              --property print.key=true \
// *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
// * }</pre>
// * You should see output data similar to below. Please note that the exact output
// * sequence will depend on how fast you type the above sentences. If you type them
// * slowly, you are likely to get each count update, e.g., kafka 1, kafka 2, kafka 3.
// * If you type them quickly, you are likely to get fewer count updates, e.g., just kafka 3.
// * This is because the commit interval is set to 10 seconds. Anything typed within
// * that interval will be compacted in memory.
// * <pre>
// * {@code
// * hello    1
// * kafka    1
// * streams  1
// * all      1
// * streams  2
// * lead     1
// * to       1
// * join     1
// * kafka    3
// * summit   1
// * }</pre>
// * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
// * also stop the Kafka broker ({@code Ctrl-C}), and only then stop the ZooKeeper instance (`{@code Ctrl-C}).
// */
//
////        streamsConfiguration.put("schema.registry.url", "http://192.168.33.10:8081");
////
////        streamsConfiguration.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
////                Serdes.String().getClass());
////        streamsConfiguration.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
////                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
////
////
////        // Specify default (de)serializers for record keys and for record values.
////        streamsConfiguration.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
////                Serdes.String().getClass());
////        streamsConfiguration.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
////                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
//public class WordCountLambdaExample {
//
//    public static void main(final String[] args) throws Exception {
//        final Properties streamsConfiguration = new Properties();
//        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
//        // against which the application is run.
//        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "1wordcount-lambda-example");
//        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "1");
//        // Where to find Kafka broker(s).
//        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.33.10:9092");
//        // Where to find the corresponding ZooKeeper ensemble.
//        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "192.168.33.10:2181");
//        // Specify default (de)serializers for record keys and for record values.
//        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
//        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
//
//        // Records should be flushed every 10 seconds. This is less than the default
//        // in order to keep this example interactive.
//        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
//        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
//        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        streamsConfiguration.put("schema.registry.url", "http://192.168.33.10:8081");
//
//        // Set up serializers and deserializers, which we will use for overriding the default serdes
//        // specified above.
////        final Serde<String> stringSerde = Serdes.String();
////        final Serde<Long> longSerde = Serdes.Long();
////        final Serde<User> userSerde = new SpecificAvroSerde<>();
////        final Serde<UserGameScoreValue> userGameScoreValueSerde = new SpecificAvroSerde<>();
////        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());
//
//
//        // In the subsequent lines we define the processing topology of the Streams application.
//        final KStreamBuilder builder = new KStreamBuilder();
//
//        // Construct a `KStream` from the input topic "TextLinesTopic", where message values
//        // represent lines of text (for the sake of this example, we ignore whatever may be stored
//        // in the message keys).
//        //
//        // Note: We could also just call `builder.stream("TextLinesTopic")` if we wanted to leverage
//        // the default serdes specified in the Streams configuration above, because these defaults
//        // match what's in the actual topic.  However we explicitly set the deserializers in the
//        // call to `stream()` below in order to show how that's done, too.
//        final KStream<User, UserGameScoreValue> userGameScoreValues = builder.stream("read");
//
//        userGameScoreValues.map((key, value) -> new KeyValue<>(new UserScore(key.getUserID(), value.getScoreType()), new ScoreValue(value.getScoreValue())))
//                           .groupByKey()
//                           .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "1table")
//                           .print();
//
//        userGameScoreValues.map((key, value) -> new KeyValue<>(new UserGameScore(key.getUserID(), value.getGameID(), value.getScoreType()), new ScoreValue(value.getScoreValue())))
//                           .groupByKey()
//                           .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "1table2")
//                           .print();
//
//        userGameScoreValues.mapValues(value -> new ScoreValue(value.getScoreValue()))
//                           .groupByKey()
//                           .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "1table3")
////                           .print();
//                           .toStream()
//                           .flatMap((key, value) -> awardTo(key, value))
//                           .print();
//
////        final KStream<String, Long>
////            textLines
////                // Split each text line, by whitespace, into words.  The text lines are the record
////                // values, i.e. we can ignore whatever data is in the record keys and thus invoke
////                // `flatMapValues()` instead of the more generic `flatMap()`.
////                // Count the occurrences of each word (record key).
////                //
////                // This will change the stream type from `KStream<String, String>` to `KTable<String, Long>`
////                // (word -> count).  In the `count` operation we must provide a name for the resulting KTable,
////                // which will be used to name e.g. its associated state store and changelog topic.
////                .map((key, value) -> )
////                .groupBy((key, value) -> (key, value.))
////                .count("table")
//////                // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
//////                .toStream();
////
////        .print();
//
//        // Write the `KStream<String, Long>` to the output topic.
////        wordCounts.to(stringSerde, longSerde, "WordsWithCountsTopic");
//
//        // Now that we have finished the definition of the processing topology we can actually run
//        // it via `start()`.  The Streams application as a whole can be launched just like any
//        // normal Java application that has a `main()` method.
//        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
//        streams.start();
//
//        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
//        Runtime.getRuntime()
//               .addShutdownHook(new Thread(streams::close));
//    }
//
//    private static <K1, V1> Iterable<KeyValue<K1, V1>> awardTo(User key, ScoreValue value) {
//        return value.getValue() < 1000
//                ? Collections.emptyList()
//                : Collections.singletonList(new KeyValue(key, new Award(">1000 AWARD")));
//    }
//
//}
