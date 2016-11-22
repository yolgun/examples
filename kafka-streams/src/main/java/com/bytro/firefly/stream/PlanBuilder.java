package com.bytro.firefly.stream;

import com.bytro.firefly.avro.Award;
import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScoreValue;
import com.bytro.firefly.avro.Value;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Collections;

import static com.bytro.firefly.data.AvroUtils.*;

/**
 * Created by yoldeta on 2016-11-22.
 */
public class PlanBuilder {
    public static final String FROM_GAME_SERVERS = "read";
    public static final String USER_SCORE_STORE = "userScoreAcc";
    public static final String USER_GAME_SCORE_STORE = "userGameScoreAcc";
    public static final String USER_STORE = "UserAcc";
    public static final String TO_KAFKA_RANKS = "UserRanking_v2";

    private PlanBuilder() {
    }

    public static TopologyBuilder prepare() {
        final KStreamBuilder build = new KStreamBuilder();

        final KStream<User, UserGameScoreValue> userGameScoreValues = build.stream(FROM_GAME_SERVERS);

        userGameScoreValues.map(toUserScoreWithValue)
                .groupByKey()
                .reduce(addValues, USER_SCORE_STORE)
                .print();

        userGameScoreValues.map(toUserGameScoreWithValue)
                .groupByKey()
                .reduce(addValues, USER_GAME_SCORE_STORE)
                .print();

        KStream<User, Value> reducedUser = userGameScoreValues.mapValues(toValue)
                .groupByKey()
                .reduce(addValues, USER_STORE)
                .toStream();
        reducedUser.print();
        reducedUser.flatMap((key, value) -> awardTo(key, value))
                .print();

        reducedUser.to(TO_KAFKA_RANKS);
        reducedUser.process(toRanking());

        return build;
    }

    private static <K1, V1> Iterable<KeyValue<K1, V1>> awardTo(User key, Value value) {
        return value.getValue() < 1000
                ? Collections.emptyList()
                : Collections.singletonList(new KeyValue(key, new Award(">1000 AWARD")));
    }

    private static ProcessorSupplier<User, Value> toRanking() {
        return RankProcessor::getInstance;
    }
}
