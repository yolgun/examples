package com.bytro.firefly.stream;

import com.bytro.firefly.avro.*;
import com.bytro.firefly.data.JsonSerializer;
import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.examples.streams.utils.SpecificAvroSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import static com.bytro.firefly.data.AvroUtils.addValues;
import static com.bytro.firefly.data.AvroUtils.toUserScoreWithValue;

/**
 * Created by yoldeta on 2016-11-22.
 */
public class PlanBuilder_v2 {
    public static final String FROM_GAME_SERVERS = "firefly8-read";
    public static final String USER_SCORE_STORE = "userScoreAcc";
    public static final String USER_GAME_SCORE_STORE = "userGameScoreAcc";
    public static final String USER_STORE = "UserAcc";
    public static final String TO_KAFKA_RANKS = "firefly8-UserRanking_v2";

    private PlanBuilder_v2() {
    }

    public static TopologyBuilder prepare() {
        final KStreamBuilder build = new KStreamBuilder();

        HashMap<String,String> props = new HashMap<>();
        props.put("schema.registry.url", "http://192.168.101.10:8081");

        SpecificAvroSerde serdeKey = new SpecificAvroSerde();
        serdeKey.configure(props, true);

        SpecificAvroSerde serdeValue = new SpecificAvroSerde();
        serdeValue.configure(props, false);

        StateStoreSupplier awardsStore = Stores.create("awardsStore")
                .withKeys(serdeKey)
                .withValues(serdeValue)
                .persistent()
                .enableCaching()
                .build();

        build.addSource("source", FROM_GAME_SERVERS)
                .addProcessor("awarder", Awarder::new, "source")
                .addStateStore(awardsStore, "awarder")
                .addProcessor("printer", Printer::new, "awarder");

        return build;
    }
}
