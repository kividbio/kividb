// Jedis runner (run.py's contract).

import java.util.List;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.RedisProtocol;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisDataException;

public class JedisSmoke {
    public static void main(String[] args) throws Exception {
        DefaultJedisClientConfig config = DefaultJedisClientConfig.builder()
                .protocol(args[2].equals("3") ? RedisProtocol.RESP3 : RedisProtocol.RESP2)
                .socketTimeoutMillis(10_000)
                .build();
        try (Jedis jedis = new Jedis(new HostAndPort(args[0], Integer.parseInt(args[1])), config)) {
            jedis.ping();
            Smoke.run("Jedis", "6.0.0", args[3], (List<String> cmd) -> {
                byte[] name = cmd.get(0).getBytes();
                ProtocolCommand command = () -> name;
                return jedis.sendCommand(command, cmd.subList(1, cmd.size()).toArray(new String[0]));
            }, e -> e instanceof JedisDataException ? Smoke.firstWord(e.getMessage()) : null);
        }
    }
}
