package com.bytro.firefly.stream;

import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScoreValue;
import org.apache.kafka.streams.processor.AbstractProcessor;

/**
 * Created by yunus on 30.11.16.
 */
public class ScoreFlatMapper extends AbstractProcessor<User, UserGameScoreValue> {
    private final MultiStringMapper mapper;

    public ScoreFlatMapper(MultiStringMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void process(User key, UserGameScoreValue value) {
        context().forward(key,value);
        mapper.apply(value.getScoreType())
                .stream()
                .map(newType -> getNewValue(value, newType))
                .forEach(newValue -> context().forward(key, newValue));
    }

    private UserGameScoreValue getNewValue(UserGameScoreValue value, String newType) {
        return UserGameScoreValue.newBuilder(value).setScoreType(newType).build();
    }
}
