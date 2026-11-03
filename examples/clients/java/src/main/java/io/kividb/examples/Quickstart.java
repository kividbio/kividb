package io.kividb.examples;

import java.util.Map;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.SetParams;

/**
 * KiviDB with Jedis: strings, expiry, hashes, a pipeline and a transaction.
 *
 * <pre>
 *   mvn -q compile exec:java                       # KiviDB on localhost:6380
 *   KIVIDB_URL=redis://:secret@host:6380/0 mvn -q compile exec:java
 * </pre>
 */
public final class Quickstart {
    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("KIVIDB_URL", "redis://localhost:6380/0");
        try (JedisPooled jedis = new JedisPooled(url)) {
            check(jedis.ping(), "PONG");

            // Start clean: these are the keys this example uses.
            jedis.del("greeting", "session:42", "user:1", "balance");

            // Strings, with and without an expiry.
            jedis.set("greeting", "hello");
            check(jedis.get("greeting"), "hello");
            jedis.set("session:42", "alice", SetParams.setParams().ex(60));
            long ttl = jedis.ttl("session:42");
            if (ttl <= 0 || ttl > 60) {
                throw new AssertionError("unexpected TTL " + ttl);
            }

            // Hashes.
            jedis.hset("user:1", Map.of("name", "Alice", "plan", "pro"));
            check(jedis.hget("user:1", "plan"), "pro");

            // A pipeline: many commands, one round trip.
            try (Pipeline pipeline = jedis.pipelined()) {
                for (int i = 0; i < 100; i++) {
                    pipeline.set("item:" + i, Integer.toString(i));
                }
                pipeline.sync();
            }
            check(jedis.get("item:99"), "99");

            // A transaction: MULTI/EXEC.
            jedis.set("balance", "100");
            try (AbstractTransaction tx = jedis.multi()) {
                tx.decrBy("balance", 30);
                tx.exec();
            }
            check(jedis.get("balance"), "70");
        }
        System.out.println("java quickstart: ok");
    }

    private static void check(String got, String want) {
        if (!want.equals(got)) {
            throw new AssertionError("got " + got + ", want " + want);
        }
    }
}
