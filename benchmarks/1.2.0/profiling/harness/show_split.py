"""show_split.py CPU_SPLIT_CSV: print categorize.py's output as text."""
import csv
import sys

cur = None
for r in csv.DictReader(open(sys.argv[1])):
    k = (r["engine"], r["workload"])
    if k != cur:
        cur = k
        print(f"\n{k[0]} {k[1]}: {r['total_us_per_op']} us/request, no call chain in {r['no_stack_pct']}% of samples")
    if float(r["share_pct"]) > 0:
        print(f"   {r['share_pct']:>5}% {r['us_per_op']:>6} us  {r['category']}")
