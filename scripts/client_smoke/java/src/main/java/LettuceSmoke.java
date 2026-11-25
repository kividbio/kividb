// Lettuce runner (run.py's contract).

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;
import io.lettuce.core.protocol.ProtocolVersion;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

public class LettuceSmoke {
    public static void main(String[] args) throws Exception {
        RedisClient client = RedisClient.create(RedisURI.Builder.redis(args[0], Integer.parseInt(args[1]))
                .withTimeout(Duration.ofSeconds(10)).build());
        client.setOptions(ClientOptions.builder()
                .protocolVersion(args[2].equals("3") ? ProtocolVersion.RESP3 : ProtocolVersion.RESP2)
                .autoReconnect(false)
                .build());
        try (StatefulRedisConnection<byte[], byte[]> conn = client.connect(ByteArrayCodec.INSTANCE)) {
            conn.sync().ping();
            Smoke.run("Lettuce", RedisClient.class.getPackage().getImplementationVersion(), args[3], (List<String> cmd) -> {
                byte[] name = cmd.get(0).getBytes(StandardCharsets.UTF_8);
                ProtocolKeyword keyword = new ProtocolKeyword() {
                    public byte[] getBytes() { return name; }
                    public String name() { return cmd.get(0); }
                };
                CommandArgs<byte[], byte[]> a = new CommandArgs<>(ByteArrayCodec.INSTANCE);
                for (String s : cmd.subList(1, cmd.size())) a.add(s.getBytes(StandardCharsets.UTF_8));
                return conn.sync().dispatch(keyword, new TreeOutput(), a);
            }, e -> e instanceof RedisCommandExecutionException ? Smoke.firstWord(e.getMessage()) : null);
        } finally {
            client.shutdown();
        }
    }
}
