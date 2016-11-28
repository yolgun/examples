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
        checkAwards(key, newValue);
        context.forward(key, newValue, "userScoreSink");
        context.forward(key, newValue, "printer");
    }

    private void checkAwards(UserScore key, Value newValue) {
        for (AwardChecker checker : awardContainer) {
            UserAward userAward = new UserAward(key.getUserID(), checker.getID());
            Optional<AwardResult> awardResult = Optional.ofNullable(userAwardStore.get(userAward));
            if (!awardResult.isPresent() || !awardResult.get().getAwardResult().equals(1.0)) {
                checker.getResult(key, newValue)
                       .ifPresent(result1 -> {
                           userAwardStore.put(result1.key, result1.value);
                           if (result1.value.getAwardResult().equals(1.0)) {
                               System.err.println("-----AWARD:" + result1);
                               context.forward(result1.key, result1.value, "userAwardSink");
                           }
                       });
            }
        }
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
