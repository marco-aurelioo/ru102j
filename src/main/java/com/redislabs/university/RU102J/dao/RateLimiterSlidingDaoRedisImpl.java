package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.*;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        String key = KeyHelper.getKey("limiter:"+windowSizeMS+":"+name+":"+maxHits);
        try(Jedis jedis = jedisPool.getResource()){
            long currentWindowsStart = System.currentTimeMillis() - windowSizeMS;

            Transaction transaction = jedis.multi();
            transaction.zadd(key, System.currentTimeMillis(),  System.currentTimeMillis()+"-"+ Math.random());
            transaction.zremrangeByScore(key, 0, currentWindowsStart);
            transaction.lpush()
            Response<Long> zcard = transaction.zcard(key);
            transaction.exec();

            if(zcard.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }


}
