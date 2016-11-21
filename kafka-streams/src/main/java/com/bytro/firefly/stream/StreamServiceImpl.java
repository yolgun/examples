package com.bytro.firefly.stream;

import com.bytro.firefly.avro.ScoreValue;
import com.bytro.firefly.avro.User;
import com.bytro.firefly.avro.UserGameScore;
import com.bytro.firefly.avro.UserGameScoreValue;
import com.bytro.firefly.avro.UserScore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * Created by yoldeta on 2016-11-21.
 */
public class StreamServiceImpl extends StreamService {
	@Override
	protected TopologyBuilder createBuilder() {
		final KStreamBuilder builder = new KStreamBuilder();

		final KStream<User, UserGameScoreValue> userGameScoreValues = builder.stream("read");

		userGameScoreValues.map((key, value) -> new KeyValue<>(new UserScore(key.getUserID(), value.getScoreType()), new ScoreValue(value.getScoreValue())))
						   .groupByKey()
						   .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "userScoreAcc")
						   .print();

		userGameScoreValues.map((key, value) -> new KeyValue<>(new UserGameScore(key.getUserID(), value.getGameID(), value.getScoreType()), new ScoreValue(value.getScoreValue())))
						   .groupByKey()
						   .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "userGameScoreAcc")
						   .print();

		KStream<User, ScoreValue> reducedUser = userGameScoreValues.mapValues(value -> new ScoreValue(value.getScoreValue()))
																   .groupByKey()
																   .reduce((value1, value2) -> new ScoreValue(value1.getValue() + value2.getValue()), "UserAcc")
																   .toStream();
		reducedUser.print();
		reducedUser.flatMap((key, value) -> awardTo(key, value))
				   .print();

		reducedUser.to("UserRanking_v2");
		reducedUser.process(() -> new RedisProcessorNoop());


		return builder;
	}

	private static class RedisProcessorNoop implements Processor<User, ScoreValue> {

		@Override
		public void init(ProcessorContext context) {

		}

		@Override
		public void process(User key, ScoreValue value) {

		}

		@Override
		public void punctuate(long timestamp) {

		}

		@Override
		public void close() {

		}
	}

	private static class RedisProcessor implements Processor<User, ScoreValue> {
		private RScoredSortedSet<Integer> redis;
		private RedissonClient redisson;

		public RedisProcessor() {
			Config config = new Config();
			config.useSingleServer()
				  .setAddress("192.168.101.10:6379")
				  .setPassword("foobared");
			redisson = Redisson.create(config);
		}

		@Override
		public void init(ProcessorContext context) {
			redis = redisson.getScoredSortedSet("UserRanking_v2");
		}

		@Override
		public void process(User key, ScoreValue value) {
			redis.add(value.getValue(), key.getUserID());
		}

		@Override
		public void punctuate(long timestamp) {

		}

		@Override
		public void close() {
		}
	}
}
