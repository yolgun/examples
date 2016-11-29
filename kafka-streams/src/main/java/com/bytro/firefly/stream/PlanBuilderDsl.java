package com.bytro.firefly.stream;

import com.bytro.firefly.avro.*;
import io.confluent.examples.streams.utils.SpecificAvroSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.HashMap;

import static com.bytro.firefly.data.AvroUtils.*;
import static com.bytro.firefly.stream.PlanBuilder_v2.USER_AWARD_STORE;

/**
 * Created by yoldeta on 2016-11-22.
 */
public class PlanBuilderDsl {
    public static final String FROM_GAME_SERVERS = "firefly10-read";
    public static final String USER_SCORE_STORE = "userScoreAcc";
    public static final String USER_GAME_SCORE_STORE = "userGameScoreAcc";
    public static final String USER_STORE = "UserAcc";
    public static final String TO_KAFKA_RANKS = "firefly10-UserRanking_v2";
    private static KeyValueMapper<UserScore, Value, Iterable<KeyValue<Object, Object>>> myKeyValueMapper;

    private PlanBuilderDsl() {
    }

    public static TopologyBuilder prepare() {
        final KStreamBuilder build = new KStreamBuilder();

        HashMap<String,String> props = new HashMap<>();
        props.put("schema.registry.url", "http://192.168.101.10:8081");
        SpecificAvroSerde serdeKey = new SpecificAvroSerde();
        serdeKey.configure(props, true);
        SpecificAvroSerde serdeValue = new SpecificAvroSerde();
        serdeValue.configure(props, false);

        StateStoreSupplier userAwardStore = Stores.create(USER_AWARD_STORE)
                .withKeys(serdeKey)
                .withValues(serdeValue)
                .persistent()
                .enableCaching()
                .build();

        build.addStateStore(userAwardStore, "userAwardStore");

        final KStream<User, UserGameScoreValue> userGameScoreValues = build.stream(FROM_GAME_SERVERS);

        userGameScoreValues.map(toUserScoreWithValue)
                .groupByKey()
                .reduce(addValues, USER_SCORE_STORE)
                .toStream()
                .process(toAwarding(), "userAwardStore");

        userGameScoreValues.map(toUserScoreWithValue)
                .groupByKey()
                .reduce(addValues, USER_SCORE_STORE)

//                .toStream()
//                .flatMap(awardManager)
                .print();

//        userGameScoreValues.map(toUserGameScoreWithValue)
//                .groupByKey()
//                .reduce(addValues, USER_GAME_SCORE_STORE)
//                .print();
//
//        KStream<User, Value> reducedUser = userGameScoreValues.mapValues(toValue)
//                .groupByKey()
//                .reduce(addValues, USER_STORE)
//                .toStream();
//        reducedUser.print();
//        reducedUser.flatMap((key, value) -> awardTo(key, value))
//                .print();
//
//        reducedUser.to(TO_KAFKA_RANKS);
//        reducedUser.process(toRanking());

        return build;
    }

    private static <K1, V1> Iterable<KeyValue<K1, V1>> awardTo(User key, Value value) {
        return value.getValue() < 1000
                ? Collections.emptyList()
                : Collections.singletonList(new KeyValue(key, new Award(">1000 AWARD")));
    }

    private static ProcessorSupplier<UserScore, Value> toAwarding() {
        return AwardProcessor::getInstance;
    }

    private static ProcessorSupplier<User, Value> toRanking() {
        return RankProcessor::getInstance;
    }
}
