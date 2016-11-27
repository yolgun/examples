package com.bytro.firefly.stream;

import com.bytro.firefly.avro.UserScore;
import com.bytro.firefly.avro.Value;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Created by yunus on 27.11.16.
 */
public class Printer implements Processor<UserScore,Value> {
    private KeyValueStore<UserScore, Value> kvStore;
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(UserScore key, Value value) {
        System.out.println(key + " -> "+ value);
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }
}
