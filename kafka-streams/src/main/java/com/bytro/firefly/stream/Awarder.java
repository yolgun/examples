package com.bytro.firefly.stream;

import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScoreValue;
import com.bytro.firefly.avro.UserScore;
import com.bytro.firefly.avro.Value;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

/**
 * Created by yunus on 27.11.16.
 */
public class Awarder implements Processor<User,UserGameScoreValue> {
    private KeyValueStore<UserScore, Value> kvStore;
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        kvStore = (KeyValueStore<UserScore, Value>) context.getStateStore("awardsStore");
    }

    @Override
    public void process(User user, UserGameScoreValue userGameScoreValue) {
        UserScore key = new UserScore(user.getUserID(), userGameScoreValue.getScoreType());
        Value value = new Value(userGameScoreValue.getScoreValue());
        Value oldValue = kvStore.get(key);
        Value newValue = oldValue == null ? value : new Value(oldValue.getValue() + value.getValue());
        kvStore.put(key, newValue);
        context.forward(key, newValue);
        context.commit();
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
