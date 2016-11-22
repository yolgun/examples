package com.bytro.firefly.data;

import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScore;
import com.bytro.firefly.avro.UserGameScoreValue;
import com.bytro.firefly.avro.UserScore;
import com.bytro.firefly.avro.Value;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * Created by yoldeta on 2016-11-22.
 */
public class AvroUtils {
    public static KeyValueMapper<User, UserGameScoreValue, KeyValue<UserScore, Value>> toUserScoreWithValue =
            (key, value) ->
                    new KeyValue<>(new UserScore(key.getUserID(), value.getScoreType()), new Value(value.getScoreValue()));
    public static Reducer<Value> addValues = (value1, value2) -> new Value(value1.getValue() + value2.getValue());
    public static KeyValueMapper<User, UserGameScoreValue, KeyValue<UserGameScore, Value>> toUserGameScoreWithValue =
            (key, value) ->
                    new KeyValue<>(new UserGameScore(key.getUserID(), value.getGameID(), value.getScoreType()),
                            new Value(value.getScoreValue()));
    public static ValueMapper<UserGameScoreValue, Value> toValue = value -> new Value(value.getScoreValue());

    private AvroUtils() {
    }
}
