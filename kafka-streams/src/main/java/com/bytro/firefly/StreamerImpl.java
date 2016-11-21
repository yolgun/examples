package com.bytro.firefly;

import com.bytro.firefly.avro.ScoreValue;
import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScore;
import com.bytro.firefly.avro.UserGameScoreValue;
import com.bytro.firefly.avro.UserScore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * Created by yoldeta on 2016-11-21.
 */
public class StreamerImpl extends Streamer {
    @Override
    protected TopologyBuilder createBuilder() {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<User, UserGameScoreValue> userGameScoreValues = builder.stream("read");

        userGameScoreValues.map((key, value) -> new KeyValue<>(new UserScore(key.getUserID(), value.getScoreType()), new ScoreValue(value.getScoreValue())))
                           .groupByKey()
                           .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "userScoreAcc")
                           .print();

        userGameScoreValues.map((key, value) -> new KeyValue<>(new UserGameScore(key.getUserID(), value.getGameID(), value.getScoreType()), new ScoreValue(value.getScoreValue())))
                           .groupByKey()
                           .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "userGameScoreAcc")
                           .print();

        KTable<User, ScoreValue> reducedUser = userGameScoreValues.mapValues(value -> new ScoreValue(value.getScoreValue()))
                                                                  .groupByKey()
                                                                  .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "UserAcc");
        reducedUser.print();
        reducedUser.toStream()
                   .flatMap((key, value) -> awardTo(key, value))
                   .print();

        return builder;
    }
}
