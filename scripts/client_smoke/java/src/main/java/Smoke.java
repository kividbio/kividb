// What the Java runners share: run.py's contract (host port protocol cases.json),
// the cases, and one JSON line per reply.

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class Smoke {
    static final ObjectMapper JSON = new ObjectMapper();

    interface Send {
        Object send(List<String> cmd) throws Exception;
    }

    /** Turns an exception into the error's first word, or null when it is not an error reply. */
    interface ErrorCode {
        String of(Exception e);
    }

    static JsonNode cases(String path) throws Exception {
        return JSON.readTree(Files.readString(Path.of(path)));
    }

    static String text(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    /** A reply as JSON: bytes as text, maps as [key, value] pairs, sets sorted. */
    static Object norm(Object v) throws Exception {
        if (v instanceof byte[] b) return text(b);
        if (v instanceof ByteBuffer b) {
            byte[] a = new byte[b.remaining()];
            b.duplicate().get(a);
            return text(a);
        }
        if (v instanceof Map<?, ?> m) {
            List<Object> out = new ArrayList<>();
            for (Map.Entry<?, ?> e : m.entrySet()) out.add(List.of(norm(e.getKey()), norm(e.getValue())));
            return out;
        }
        if (v instanceof Set<?> s) {
            List<Object> out = new ArrayList<>();
            for (Object x : s) out.add(norm(x));
            out.sort(Comparator.comparing(x -> {
                try {
                    return JSON.writeValueAsString(x);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
            return out;
        }
        if (v instanceof Collection<?> c) {
            List<Object> out = new ArrayList<>();
            for (Object x : c) out.add(norm(x));
            return out;
        }
        if (v instanceof redis.clients.jedis.util.KeyValue<?, ?> kv) return List.of(norm(kv.getKey()), norm(kv.getValue()));
        if (v instanceof Double d && (d.isInfinite() || d.isNaN())) return d.toString();
        return v;
    }

    static void run(String client, String version, String casesPath, Send send, ErrorCode code) throws Exception {
        Map<String, Object> head = new LinkedHashMap<>();
        head.put("client", client);
        head.put("version", version);
        System.out.println(JSON.writeValueAsString(head));
        for (JsonNode c : cases(casesPath)) {
            List<String> cmd = new ArrayList<>();
            c.get("cmd").forEach(a -> cmd.add(a.asText()));
            Map<String, Object> line = new LinkedHashMap<>();
            line.put("case", c.get("name").asText());
            try {
                line.put("reply", norm(send.send(cmd)));
            } catch (Exception e) {
                String err = code.of(e);
                if (err == null) throw e;
                line.put("error", err);
            }
            System.out.println(JSON.writeValueAsString(line));
        }
    }

    static String firstWord(String message) {
        return message == null ? "" : message.split(" ", 2)[0];
    }
}
