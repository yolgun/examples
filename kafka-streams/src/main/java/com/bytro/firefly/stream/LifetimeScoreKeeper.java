package com.bytro.firefly.stream;

import java.util.Optional;

import com.bytro.firefly.avro.AwardResult;
import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserAward;
import com.bytro.firefly.avro.UserGameScoreValue;
import com.bytro.firefly.avro.UserScore;
import com.bytro.firefly.avro.Value;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;


/**
 * Created by yunus on 27.11.16.
 */
public class LifetimeScoreKeeper implements Processor<User,UserGameScoreValue> {
    private ProcessorContext context;
    private KeyValueStore<UserScore, Value> userScoreStore;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.userScoreStore = (KeyValueStore) context.getStateStore(PlanBuilder_v2.USER_SCORE_STORE);
    }

    @Override
    public void process(User user, UserGameScoreValue userGameScoreValue) {
        UserScore key = new UserScore(user.getUserID(), userGameScoreValue.getScoreType());
        Value value = new Value(userGameScoreValue.getScoreValue());
        Value oldValue = userScoreStore.get(key);
        Value newValue = oldValue == null ? value : new Value(oldValue.getValue() + value.getValue());
        userScoreStore.put(key, newValue);
        context.forward(key, newValue);
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
