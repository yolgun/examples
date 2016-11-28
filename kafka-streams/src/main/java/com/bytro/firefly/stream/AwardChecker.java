package com.bytro.firefly.stream;

import com.bytro.firefly.avro.AwardResult;
import com.bytro.firefly.avro.UserAward;
import com.bytro.firefly.avro.UserScore;
import com.bytro.firefly.avro.Value;
import org.apache.kafka.streams.KeyValue;

import java.util.Optional;

/**
 * Created by yunus on 27.11.16.
 */
public class AwardChecker {
    private int id;

    public String getName() {
        return "Award-" + id;
    }

    private String field;
    private int goal;

    public AwardChecker(int id, String field, int goal) {
        this.id = id;
        this.field = field;
        this.goal = goal;
    }

    public boolean check(String newField, int value) {
        if (field.equals(newField)) {
            return value >= goal;
        }
        return false;
    }

    public boolean isRelatedTo(String newField) {
        return field.equals(newField);
    }

    public Optional<KeyValue<UserAward, AwardResult>> getResult(UserScore oldKey, Value oldValue) {
        if (oldKey.getScoreType().equals(field)) {
            return Optional.of(KeyValue.pair(
                    new UserAward(oldKey.getUserID(), id),
                    new AwardResult(Math.min(1.0, oldValue.getValue() * 1.0 / goal ))));
        }
        return Optional.empty();
    }

    public Integer getID() {
        return id;
    }
}
