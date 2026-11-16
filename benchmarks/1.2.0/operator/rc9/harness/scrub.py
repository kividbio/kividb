"""scrub.py <dir> <acr name>: in every text file under <dir>, replace the registry name with <acr>, IPv4 addresses
with <ip>, and workstation paths with <work>, in place."""
import os
import re
import sys

root, acr = sys.argv[1], sys.argv[2]
ip = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
paths = re.compile(r"(?:[A-Za-z]:[\\/]|/[a-z]/)Users[\\/][^\s\"'`|,;)]*", re.I)
n = 0
for d, _, fs in os.walk(root):
    for f in fs:
        p = os.path.join(d, f)
        try:
            t = open(p, encoding="utf-8").read()
        except (UnicodeDecodeError, OSError):
            continue
        u = paths.sub("<work>", ip.sub("<ip>", t.replace(acr, "<acr>")))
        if u != t:
            open(p, "w", encoding="utf-8", newline="").write(u)
            n += 1
print("scrubbed", n, "files")
