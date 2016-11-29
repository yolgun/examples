package com.bytro.firefly.stream;

import com.bytro.firefly.avro.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

/**
 * Created by yunus on 29.11.16.
 */
public class AwardMapper implements Processor<UserScore, Value > {
    private KeyValueStore<UserAward, AwardResult> userAwardStore;
    private ProcessorContext context;
    private AwardContainer awardContainer;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.userAwardStore = (KeyValueStore) context.getStateStore(PlanBuilder_v2.USER_AWARD_STORE);
        this.awardContainer = new AwardContainer();
    }

    @Override
    public void process(UserScore key, Value value) {
        for (AwardChecker checker : awardContainer) {
            UserAward userAward = new UserAward(key.getUserID(), checker.getID());
            Optional<AwardResult> awardResult = Optional.ofNullable(userAwardStore.get(userAward));
            if (!awardResult.isPresent() || !awardResult.get().getAwardResult().equals(1.0)) {
                checker.getResult(key, value)
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
