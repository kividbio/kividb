// Lettuce through Sentinel: writes to the master Sentinel names, and keeps
// writing to the new one after a failover. The contract run.sh expects of
// each client is in run.sh.

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FollowLettuce {
    public static void main(String[] args) throws Exception {
        String key = System.getenv("KEY");
        String[] sentinels = System.getenv("SENTINELS").split(",");
        String sentinelPassword = System.getenv("SENTINEL_PASSWORD");
        String[] first = sentinels[0].split(":");
        RedisURI.Builder uri = RedisURI.Builder
                .sentinel(first[0], Integer.parseInt(first[1]), System.getenv("MASTER"), sentinelPassword)
                .withPassword(System.getenv("PASSWORD").toCharArray())
                .withTimeout(Duration.ofSeconds(1));
        for (int i = 1; i < sentinels.length; i++) {
            String[] a = sentinels[i].split(":");
            uri.withSentinel(a[0], Integer.parseInt(a[1]), sentinelPassword);
        }
        long deadline = System.nanoTime() + (long) (1e9 * Double.parseDouble(
                System.getenv().getOrDefault("DEADLINE", "60")));
        Pattern tcpPort = Pattern.compile("^tcp_port:(\\d+)", Pattern.MULTILINE);

        RedisClient client = RedisClient.create(uri.build());
        try (StatefulRedisConnection<String, String> conn = client.connect()) {
            RedisCommands<String, String> r = conn.sync();
            Integer firstPort = null;
            int errors = 0, after = 0;
            while (System.nanoTime() < deadline) {
                long n;
                int port;
                try {
                    n = r.incr(key);
                    Matcher m = tcpPort.matcher(r.info("server"));
                    m.find();
                    port = Integer.parseInt(m.group(1));
                } catch (RuntimeException e) {
                    errors++;
                    Thread.sleep(100);
                    continue;
                }
                if (firstPort == null) {
                    firstPort = port;
                    System.out.println("ready " + port);
                } else if (port != firstPort && ++after >= 10) {
                    if (Long.parseLong(r.get(key)) != n) {
                        System.err.println("the new master does not read back the last write");
                        System.exit(1);
                    }
                    System.out.println("followed " + firstPort + " -> " + port + " after "
                            + errors + " failed writes");
                    System.exit(0);
                }
                Thread.sleep(100);
            }
            System.err.println("still on " + firstPort + " at the deadline, " + errors + " failed writes");
            System.exit(1);
        }
    }
}
