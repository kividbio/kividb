"""The RESULT lines of several bullmq-bench.cjs processes, as one line of sums."""
import json
import sys

add = process = both = 0
queues = failed = 0
for line in sys.stdin:
    line = line.strip()
    if not line.startswith("RESULT "):
        if line:
            failed += 1
        continue
    result = json.loads(line[len("RESULT "):])
    queues += 1
    add += result.get("add_per_s", 0)
    process += result.get("process_per_s", 0)
    both += result.get("both_per_s", 0)
    if not result.get("completed", True):
        failed += 1
print(f"add={add} process={process} both={both}" + (f" FAILED={failed}" if failed else ""))
