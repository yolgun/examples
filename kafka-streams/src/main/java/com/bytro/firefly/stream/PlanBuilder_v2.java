package com.bytro.firefly.stream;

import io.confluent.examples.streams.utils.SpecificAvroSerde;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.HashMap;

/**
 * Created by yoldeta on 2016-11-22.
 */
public class PlanBuilder_v2 {
    public static final String FROM_GAME_SERVERS = "firefly10-read";
    public static final String USER_SCORE_STORE = "userScoreAcc";
    public static final String USER_AWARD_STORE = "userAwardStore";

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

        StateStoreSupplier userScoreStore = createStore(USER_SCORE_STORE, serdeKey, serdeValue);
        StateStoreSupplier userAwardStore = createStore(USER_AWARD_STORE, serdeKey, serdeValue);

        build.addSource("source", FROM_GAME_SERVERS)
                .addProcessor("lifetimeScoreKeeper", LifetimeScoreKeeper::new, "source")
                    .addStateStore(userScoreStore, "lifetimeScoreKeeper")
                .addProcessor("printer", Printer::new, "lifetimeScoreKeeper")
                .addProcessor("awardMapper", AwardMapper::new, "lifetimeScoreKeeper")
                    .addStateStore(userAwardStore, "awardMapper")
                .addSink("userScoreSink", "firefly10-userScore", "lifetimeScoreKeeper")
                .addSink("userAwardSink", "firefly10-userAward", "awardMapper");
        return build;
    }

    private static StateStoreSupplier createStore(String storeName, SpecificAvroSerde serdeKey, SpecificAvroSerde serdeValue) {
        return Stores.create(storeName)
                .withKeys(serdeKey)
                .withValues(serdeValue)
                .persistent()
                .enableCaching()
                .build();
    }
}
