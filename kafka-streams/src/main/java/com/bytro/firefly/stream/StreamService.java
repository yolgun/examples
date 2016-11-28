package com.bytro.firefly.stream;

import io.confluent.examples.streams.utils.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program that
 * computes a simple word occurrence histogram from an input text. This example uses lambda
 * expressions and thus works with Java 8+ only.
 * <p>
 * In this example, the input stream reads from a topic named "TextLinesTopic", where the values of
 * messages represent lines of text; and the histogram output is written to topic
 * "WordsWithCountsTopic", where each record is an updated count of a single word, i.e. {@code word (String) -> currentCount (Long)}.
 * <p>
 * Note: Before running this example you must 1) create the source topic (e.g. via {@code kafka-topics --create ...}),
 * then 2) start this example and 3) write some data to the source topic (e.g. via {@code kafka-console-producer}).
 * Otherwise you won't see any data arriving in the output topic.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input and output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic TextLinesTopic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic WordsWithCountsTopic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/examples/tree/master/kafka-streams#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/streams-examples-3.1.0-standalone.jar io.confluent.examples.streams.WordCountLambdaExample
 * }</pre>
 * 4) Write some input data to the source topics (e.g. via {@code kafka-console-producer}). The already
 * running example application (step 3) will automatically process this input data and write the
 * results to the output topic.
 * <pre>
 * {@code
 * # Start the console producer. You can then enter input data by writing some line of text, followed by ENTER:
 * #
 * #   hello kafka streams<ENTER>
 * #   all streams lead to kafka<ENTER>
 * #   join kafka summit<ENTER>
 * #
 * # Every line you enter will become the value of a single Kafka message.
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic
 * }</pre>
 * 5) Inspect the resulting data in the output topics, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic WordsWithCountsTopic --from-beginning \
 *                              --zookeeper localhost:2181 \
 *                              --property print.key=true \
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }</pre>
 * You should see output data similar to below. Please note that the exact output
 * sequence will depend on how fast you type the above sentences. If you type them
 * slowly, you are likely to get each count update, e.g., kafka 1, kafka 2, kafka 3.
 * If you type them quickly, you are likely to get fewer count updates, e.g., just kafka 3.
 * This is because the commit interval is set to 10 seconds. Anything typed within
 * that interval will be compacted in memory.
 * <pre>
 * {@code
 * hello    1
 * kafka    1
 * streams  1
 * all      1
 * streams  2
 * lead     1
 * to       1
 * join     1
 * kafka    3
 * summit   1
 * }</pre>
 * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Kafka broker ({@code Ctrl-C}), and only then stop the ZooKeeper instance (`{@code Ctrl-C}).
 */
public class StreamService {

    private KafkaStreams streams;

    public void start() {
        Properties streamConfiguration = createProperties();
        TopologyBuilder plan = PlanBuilder_v2.prepare();
        startStreaming(plan, streamConfiguration);
    }

    private Properties createProperties() {
        final Properties streamsConfiguration = new Properties();
        final String HOSTNAME = "192.168.101.10";
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "firefly10");
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "1");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, HOSTNAME + ":9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, HOSTNAME + ":2181");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        // Records should be flushed every 10 seconds. This is less than the default
        // in order to keep this example interactive.
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put("schema.registry.url", "http://" + HOSTNAME + ":8081");
        return streamsConfiguration;
    }

    private void startStreaming(TopologyBuilder plan, Properties streamsConfiguration) {
        streams = new KafkaStreams(plan, streamsConfiguration);
        streams.cleanUp();
        streams.start();
    }

    public void stop() {
        streams.close();
    }

    public KafkaStreams getStreams() {
        return streams;
    }
}