#!/usr/bin/env python3
"""
Firecrawl scrape, map, batch scrape and crawl jobs on a running self-hosted Firecrawl, against the
local test site, with the Redis-protocol server restarted in the middle of a crawl.

Run by run_snips.sh under Firecrawl's own harness, after its test suite (EXTRA_CMD), so the API and
every worker are up:

  EXTRA_CMD="python3 $PWD/firecrawl_e2e.py" FC=~/firecrawl ./run_snips.sh kividb

Standard library only. It reads FC_LABEL and FC_REDIS_PORT from the environment run_snips.sh sets,
restarts the server container (fc-redis, or FC_SERVER_CONTAINER) with `docker restart`, and writes e2e-<label>.json next to
this script.

Checks:
  * map of the test site finds its pages
  * 40 scrapes at once all come back with markdown
  * a batch scrape of 20 pages completes with all 20
  * a crawl of the whole site completes; the number of pages is the reference for the next one
  * a crawl during which the server is restarted completes with the same pages, none twice
  * scrapes still work after the restart
  * the server reports maxmemory-policy noeviction (BullMQ warns otherwise)
"""
import concurrent.futures
import json
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
API = os.environ.get("TEST_API_URL", "http://127.0.0.1:3002")
SITE = os.environ.get("TEST_SUITE_WEBSITE", "http://127.0.0.1:4321")
LABEL = os.environ.get("FC_LABEL", "unknown")
PORT = int(os.environ.get("FC_REDIS_PORT", "6379"))
CONTAINER = os.environ.get("FC_SERVER_CONTAINER", "fc-redis")  # what `docker restart` restarts
results = []


def log(msg):
    print(f"[e2e {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def check(name, ok, detail=""):
    results.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})
    log(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}")


def call(method, path, body=None, timeout=120):
    url = path if path.startswith("http") else API + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Content-Type": "application/json", "Authorization": "Bearer fc-selfhosted"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except ValueError:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)}


def resp_cmd(*args):
    """One command over a raw socket: the harness needs nothing but the standard library."""
    out = b"*%d\r\n" % len(args)
    for a in args:
        a = a.encode()
        out += b"$%d\r\n%s\r\n" % (len(a), a)
    s = socket.create_connection(("127.0.0.1", PORT), timeout=5)
    s.sendall(out)
    time.sleep(0.2)
    data = s.recv(65536)
    s.close()
    return data.decode(errors="replace")


def poll_job(kind, job_id, timeout=300):
    """Poll /v2/<kind>/<id> until it ends; return (status, every document's URL)."""
    end = time.time() + timeout
    st = {}
    while time.time() < end:
        code, st = call("GET", f"/v2/{kind}/{job_id}")
        if code == 200 and st.get("status") in ("completed", "failed", "cancelled"):
            break
        time.sleep(2)
    urls = []
    page = st
    while True:
        for d in page.get("data") or []:
            m = d.get("metadata") or {}
            u = m.get("sourceURL") or m.get("url") or ""
            # The crawler records a page under the first form of its URL it meets, with or without
            # the trailing slash, so compare without it.
            urls.append(u.rstrip("/") or u)
        nxt = page.get("next")
        if not nxt:
            break
        code, page = call("GET", nxt)
        if code != 200:
            break
    return st.get("status"), urls, st


def main():
    policy = resp_cmd("CONFIG", "GET", "maxmemory-policy")
    check("server reports maxmemory-policy noeviction (BullMQ warns otherwise)", "noeviction" in policy,
          policy.replace("\r\n", " ").strip())

    code, m = call("POST", "/v2/map", {"url": SITE, "limit": 200})
    links = [l["url"] if isinstance(l, dict) else l for l in (m.get("links") or [])]
    pages = sorted({l for l in links if l.startswith(SITE) and not l.endswith((".xml", ".txt"))})
    check("map of the test site finds its pages", code == 200 and len(pages) >= 10, f"HTTP {code}, {len(pages)} pages")

    if len(pages) < 10:
        # Behind a Docker network alias, map finds nothing: take the pages from a crawl, so the other
        # checks still run.
        code, c = call("POST", "/v2/crawl", {"url": SITE, "limit": 300})
        _, crawled, _ = poll_job("crawl", c.get("id"))
        pages = sorted(set(crawled))
        log(f"map found too few pages; {len(pages)} pages from a crawl instead")

    targets = (pages * 4)[:40]

    def scrape(u):
        c, r = call("POST", "/v2/scrape", {"url": u, "formats": ["markdown"]})
        return c == 200 and bool((r.get("data") or {}).get("markdown"))

    with concurrent.futures.ThreadPoolExecutor(40) as ex:
        oks = list(ex.map(scrape, targets))
    check("40 scrapes at once all return markdown", len(oks) == 40 and all(oks), f"{sum(oks)}/{len(oks)}")

    code, b = call("POST", "/v2/batch/scrape", {"urls": pages[:20], "formats": ["markdown"]})
    status, urls, st = poll_job("batch/scrape", b.get("id"))
    check("batch scrape of 20 pages completes with all 20", status == "completed" and len(urls) == min(20, len(pages)),
          f"status {status}, {len(urls)} documents")

    code, c = call("POST", "/v2/crawl", {"url": SITE, "limit": 300})
    t0 = time.time()
    status, ref_urls, st = poll_job("crawl", c.get("id"))
    ref = sorted(set(ref_urls))
    check("crawl of the test site completes", status == "completed" and len(ref) >= 10,
          f"status {status}, {len(ref_urls)} pages ({len(ref)} distinct) in {time.time() - t0:.0f}s")

    code, c = call("POST", "/v2/crawl", {"url": SITE, "limit": 300})
    time.sleep(3)
    t1 = time.time()
    subprocess.run(["docker", "restart", "-t", "30", CONTAINER], check=True, capture_output=True)
    restart_s = time.time() - t1
    status, got, st = poll_job("crawl", c.get("id"), timeout=600)
    dups = len(got) - len(set(got))
    check("crawl with the server restarted 3 s in completes with the same pages, none twice",
          status == "completed" and sorted(set(got)) == ref and dups == 0,
          f"restart took {restart_s:.1f}s; status {status}, {len(got)} pages, {dups} twice, "
          f"missing {sorted(set(ref) - set(got))[:5]}, extra {sorted(set(got) - set(ref))[:5]}")

    with concurrent.futures.ThreadPoolExecutor(10) as ex:
        oks = list(ex.map(scrape, pages[:10]))
    check("scrapes still work after the restart", len(oks) == 10 and all(oks), f"{sum(oks)}/{len(oks)}")

    with open(os.path.join(HERE, f"e2e-{LABEL}.json"), "w") as f:
        json.dump({"label": LABEL, "checks": results}, f, indent=2)


if __name__ == "__main__":
    main()
