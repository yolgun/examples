package com.bytro.firefly.stream;

import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.Value;
import com.bytro.firefly.redis.RedisFactory;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.redisson.api.RScoredSortedSet;

public class RankProcessor implements Processor<User, Value> {
    public static final RankProcessor instance = new RankProcessor();
    private RScoredSortedSet<Integer> ranks;

    private RankProcessor() {
    }

    public static RankProcessor getInstance() {
        return instance;
    }

    @Override
    public void init(ProcessorContext context) {
        ranks = RedisFactory.getInstance().getScoredSortedSet("firefly10-ranks");
    }

    @Override
    public void process(User key, Value value) {
        ranks.add(value.getValue(), key.getUserID());
    }

    @Override
    public void punctuate(long timestamp) {
    }

    @Override
    public void close() {
    }
}
