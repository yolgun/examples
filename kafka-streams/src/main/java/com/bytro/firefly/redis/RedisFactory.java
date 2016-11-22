package com.bytro.firefly.redis;

import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * Created by yoldeta on 2016-11-22.
 */
public class RedisFactory {
    private static final RedisFactory instance = new RedisFactory();
    private ConcurrentHashMap<String, RScoredSortedSet<Integer>> sets;
    private RedissonClient client;

    private RedisFactory() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("192.168.101.10:6379")
                .setPassword("foobared");
        this.client = Redisson.create(config);
    }

    public static RedisFactory getInstance() {
        return instance;
    }

    public RScoredSortedSet<Integer> getScoredSortedSet(String newSet) {
        return sets.computeIfAbsent(newSet, client::getScoredSortedSet);
    }
}
