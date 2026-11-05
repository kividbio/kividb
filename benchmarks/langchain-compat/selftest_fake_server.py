#!/usr/bin/env python3
"""
A server that answers PING, INFO, SCAN and HELLO and refuses every other command.

It plays a server with no FT.*, JSON.* or plain key commands, to show that the harness
can fail and reports why, instead of passing whatever it is pointed at:

  python selftest_fake_server.py &                      # listens on 127.0.0.1:6397
  python langchain_kividb_compat.py --url redis://127.0.0.1:6397 --label fake
  # expected: 1 PASS (PING) and 5 FAIL, no traceback; then delete results-fake.json
"""
import socketserver

PORT = 6397
HELLO = (b"%3\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$5\r\n7.0.0\r\n"
         b"$5\r\nproto\r\n:3\r\n")


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        while True:
            line = self.rfile.readline()
            if not line:
                return
            if not line.startswith(b"*"):
                continue
            args = []
            for _ in range(int(line[1:])):
                size = int(self.rfile.readline()[1:])
                args.append(self.rfile.read(size))
                self.rfile.read(2)
            cmd = args[0].upper()
            if cmd == b"PING":
                out = b"+PONG\r\n"
            elif cmd == b"HELLO":
                out = HELLO
            elif cmd == b"INFO":
                body = b"# Server\r\nredis_version:0.0.0\r\n"
                out = b"$%d\r\n%s\r\n" % (len(body), body)
            elif cmd == b"SCAN":
                out = b"*2\r\n$1\r\n0\r\n*0\r\n"
            else:
                out = b"-ERR unknown command '%s'\r\n" % args[0]
            self.wfile.write(out)


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    Server(("127.0.0.1", PORT), Handler).serve_forever()
