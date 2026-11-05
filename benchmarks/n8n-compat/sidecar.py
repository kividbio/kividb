#!/usr/bin/env python3
"""
The HTTP service the test workflow calls: it plays the slow, sometimes failing, outside system a real
n8n workflow talks to, and it records what reached it, so the harness can tell whether each execution
ran exactly once.

  POST /work  {"batch": B, "id": N, "exec": E}  sleeps WORK_MS, then answers 200, or 500 when N says so:
                N % 10 == 0  always 500 (the execution ends in error after n8n's 3 tries)
                N % 10 == 5  500 on the first try only (n8n's retry makes it succeed)
  POST /done  {"batch": B, "id": N, "exec": E}  the workflow's last node: the execution got to the end
  GET  /stats                                   everything recorded, as JSON
  POST /reset                                   forget everything

Standard library only; it runs in a plain python:3.12-slim container.
"""
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WORK_MS = int(os.environ.get("WORK_MS", "2000"))
lock = threading.Lock()
work = {}  # "batch:id" -> list of {"exec", "t"}
done = {}  # "batch:id" -> list of {"exec", "t"}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def do_GET(self):
        if self.path == "/stats":
            with lock:
                self.reply(200, {"work": work, "done": done})
        else:
            self.reply(404, {})

    def do_POST(self):
        if self.path == "/reset":
            with lock:
                work.clear()
                done.clear()
            self.reply(200, {})
            return
        b = self.body()
        key = f"{b.get('batch')}:{b.get('id')}"
        entry = {"exec": str(b.get("exec")), "t": time.time()}
        if self.path == "/work":
            with lock:
                tries = work.setdefault(key, [])
                tries.append(entry)
                attempt = len(tries)
            time.sleep(WORK_MS / 1000)
            n = int(b.get("id"))
            if n % 10 == 0 or (n % 10 == 5 and attempt == 1):
                self.reply(500, {"error": "planned failure", "attempt": attempt})
            else:
                self.reply(200, {"ok": True, "attempt": attempt})
        elif self.path == "/done":
            with lock:
                done.setdefault(key, []).append(entry)
            self.reply(200, {"ok": True})
        else:
            self.reply(404, {})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8099), Handler).serve_forever()
