// A Lettuce output that keeps any reply whole: arrays as lists, RESP3 maps as
// maps and sets as sets, doubles and booleans as they come. Lettuce's own
// ObjectOutput fails on a reply that is not an array; the depth handling is
// NestedMultiOutput's.

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.CommandOutput;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

final class TreeOutput extends CommandOutput<byte[], byte[], Object> {
    private static final class Node {
        final String kind;
        final List<Object> items = new ArrayList<>();

        Node(String kind) {
            this.kind = kind;
        }
    }

    private final Deque<Node> stack = new ArrayDeque<>();
    private Node current;
    private int depth;

    TreeOutput() {
        super(ByteArrayCodec.INSTANCE, null);
    }

    private void add(Object v) {
        if (current == null) output = v;
        else current.items.add(v);
    }

    private static byte[] bytes(ByteBuffer b) {
        byte[] a = new byte[b.remaining()];
        b.duplicate().get(a);
        return a;
    }

    @Override public void set(ByteBuffer b) { add(b == null ? null : bytes(b)); }
    @Override public void setSingle(ByteBuffer b) { add(b == null ? null : bytes(b)); }
    @Override public void setBigNumber(ByteBuffer b) { add(b == null ? null : new String(bytes(b))); }
    @Override public void set(long v) { add(v); }
    @Override public void set(double v) { add(v); }
    @Override public void set(boolean v) { add(v); }

    private void open(String kind) {
        Node n = new Node(kind);
        if (current == null) {
            output = n;
        } else {
            current.items.add(n);
            stack.push(current);
        }
        current = n;
        depth++;
    }

    @Override public void multi(int count) { open("array"); }
    @Override public void multiArray(int count) { open("array"); }
    @Override public void multiPush(int count) { open("array"); }
    @Override public void multiSet(int count) { open("set"); }
    @Override public void multiMap(int count) { open("map"); }

    @Override
    public void complete(int depth) {
        if (depth > 0 && depth < this.depth) {
            current = stack.pop();
            this.depth--;
        }
    }

    @Override
    public Object get() {
        return convert(output);
    }

    private static Object convert(Object v) {
        if (!(v instanceof Node n)) return v;
        switch (n.kind) {
            case "map": {
                Map<Object, Object> m = new LinkedHashMap<>();
                for (int i = 0; i + 1 < n.items.size(); i += 2) m.put(convert(n.items.get(i)), convert(n.items.get(i + 1)));
                return m;
            }
            case "set": {
                LinkedHashSet<Object> s = new LinkedHashSet<>();
                for (Object x : n.items) s.add(convert(x));
                return s;
            }
            default: {
                List<Object> l = new ArrayList<>();
                for (Object x : n.items) l.add(convert(x));
                return l;
            }
        }
    }
}
