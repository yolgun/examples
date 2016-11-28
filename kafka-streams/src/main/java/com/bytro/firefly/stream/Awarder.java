package com.bytro.firefly.stream;

import com.bytro.firefly.avro.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;


/**
 * Created by yunus on 27.11.16.
 */
public class Awarder implements Processor<User,UserGameScoreValue> {
    private KeyValueStore<UserScore, Value> userScoreStore;
    private KeyValueStore<UserAward, AwardResult> userAwardStore;
    private ProcessorContext context;
    private AwardContainer awardContainer;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.userScoreStore = (KeyValueStore) context.getStateStore(PlanBuilder_v2.USER_SCORE_STORE);
        this.userAwardStore = (KeyValueStore) context.getStateStore(PlanBuilder_v2.USER_AWARD_STORE);
        this.awardContainer = new AwardContainer();
    }

    @Override
    public void process(User user, UserGameScoreValue userGameScoreValue) {
        UserScore key = new UserScore(user.getUserID(), userGameScoreValue.getScoreType());
        Value value = new Value(userGameScoreValue.getScoreValue());
        Value oldValue = userScoreStore.get(key);
        Value newValue = oldValue == null ? value : new Value(oldValue.getValue() + value.getValue());
        userScoreStore.put(key, newValue);
        for (AwardChecker checker : awardContainer) {
            UserAward userAward = new UserAward(key.getUserID(), checker.getID());
            Optional<AwardResult> awardResult = Optional.ofNullable(userAwardStore.get(userAward));
            if (!awardResult.isPresent() || !awardResult.get().getAwardResult().equals(1.0)) {
                Optional<KeyValue<UserAward, AwardResult>> result = checker.getResult(key, newValue);
                result.ifPresent(result1 -> {
                    userAwardStore.put(result1.key, result1.value);
                    if (result1.value.getAwardResult().equals(1.0)) {
                        System.err.println("-----AWARD:" + result1);
                    }
                });
            }
        }
        context.forward(key, newValue);
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
