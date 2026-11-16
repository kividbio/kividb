#!/usr/bin/env python3
"""
A stand-in for an OpenAI-compatible model server: offline, deterministic, no API key, no spend.

  python stub_openai.py --port 9999            # serves http://0.0.0.0:9999/v1

It answers the three calls the compatibility harnesses need:

  GET  /v1/models              two models, "stub-chat" and "stub-embed"
  POST /v1/embeddings          a hashed bag of words, unit length (--dim, default 768)
  POST /v1/chat/completions    "stub answer: " plus the start of the last user message,
                               streamed (SSE) or not, with a usage block

The embedding proves the wiring (a document is embedded, stored, searched), not the quality of
a real model's results. The chat answer is fixed text, so a test can check it came back.
"""
import argparse
import hashlib
import json
import math
import re
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DIM = 768
CALLS = {"embeddings": 0, "chat": 0}


def embed(text):
    v = [0.0] * DIM
    for tok in re.findall(r"[a-z0-9]+", str(text).lower()):
        v[int(hashlib.md5(tok.encode()).hexdigest(), 16) % DIM] += 1.0
    if not any(v):
        v[0] = 1.0
    n = math.sqrt(sum(x * x for x in v))
    return [x / n for x in v]


def last_user_text(messages):
    for m in reversed(messages or []):
        if m.get("role") == "user":
            c = m.get("content")
            if isinstance(c, list):
                c = " ".join(p.get("text", "") for p in c if isinstance(p, dict))
            return str(c or "")
    return ""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # quiet
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            return json.loads(raw or b"{}")
        except ValueError:
            return {}

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path.endswith("/models"):
            now = int(time.time())
            self._json(200, {"object": "list", "data": [
                {"id": "stub-chat", "object": "model", "created": now, "owned_by": "stub"},
                {"id": "stub-embed", "object": "model", "created": now, "owned_by": "stub"}]})
        elif path in ("", "/health", "/v1"):
            self._json(200, {"ok": True, "calls": CALLS})
        else:
            self._json(404, {"error": {"message": "not found"}})

    def do_POST(self):
        path = self.path.split("?")[0].rstrip("/")
        req = self._body()
        if path.endswith("/embeddings"):
            CALLS["embeddings"] += 1
            inp = req.get("input", "")
            items = inp if isinstance(inp, list) else [inp]
            data = [{"object": "embedding", "index": i, "embedding": embed(t)} for i, t in enumerate(items)]
            toks = sum(len(str(t).split()) for t in items)
            self._json(200, {"object": "list", "data": data, "model": req.get("model", "stub-embed"),
                             "usage": {"prompt_tokens": toks, "total_tokens": toks}})
        elif path.endswith("/chat/completions"):
            CALLS["chat"] += 1
            answer = "stub answer: " + last_user_text(req.get("messages"))[:120]
            cid, now, model = "chatcmpl-" + uuid.uuid4().hex[:12], int(time.time()), req.get("model", "stub-chat")
            usage = {"prompt_tokens": 10, "completion_tokens": len(answer.split()),
                     "total_tokens": 10 + len(answer.split())}
            if req.get("stream"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                words = answer.split(" ")
                for i, w in enumerate(words):
                    chunk = {"id": cid, "object": "chat.completion.chunk", "created": now, "model": model,
                             "choices": [{"index": 0, "delta": ({"role": "assistant"} if i == 0 else {}) |
                                          {"content": w + (" " if i < len(words) - 1 else "")},
                                          "finish_reason": None}]}
                    self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
                end = {"id": cid, "object": "chat.completion.chunk", "created": now, "model": model,
                       "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}], "usage": usage}
                self.wfile.write(f"data: {json.dumps(end)}\n\n".encode())
                self.wfile.write(b"data: [DONE]\n\n")
                self.wfile.flush()
                self.close_connection = True
            else:
                self._json(200, {"id": cid, "object": "chat.completion", "created": now, "model": model,
                                 "choices": [{"index": 0, "finish_reason": "stop",
                                              "message": {"role": "assistant", "content": answer}}],
                                 "usage": usage})
        else:
            self._json(404, {"error": {"message": "not found"}})


def main():
    global DIM
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9999)
    ap.add_argument("--dim", type=int, default=768)
    args = ap.parse_args()
    DIM = args.dim
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
