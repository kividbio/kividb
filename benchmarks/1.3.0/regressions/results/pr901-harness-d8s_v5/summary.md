# #901: the cost of one script call (harness and raw data)

The measurements behind docs/PERFORMANCE.md "Script call cost" and the #901
PR, on one Azure Standard_D8s_v5 (Ubuntu 24.04, rustc 1.100.0, the release
profile, `lua,tls`), server pinned to CPUs 0-1, clients on 2-7. `raw.tar.gz`:

- `harness/`: `main.go` (the Go client: Tyk's sliding log with a synthetic
  clock, `return 1`, the lock, centrifuge's history scripts, `-tag`, and an
  `emit` mode for callgrind), `bullcpu.mjs` (bullmq 5.81.5), `bench901.sh`
  (wall clock: calls/s, server CPU per call, `usec_per_call`, runner
  hand-offs), `cg2.sh`/`cg3.sh` (instructions per call under callgrind,
  `CMP=plain`), `cgr.sh` (Redis by the difference of two runs), `hop.sh`
  (per-thread CPU: the script-runner hand-off), `fuzz901.py`/`fuzzrun.sh`
  (the differential fuzz), `repl901.sh`/`dig.py` (master, replica and AOF
  reload digests), `gcq.py` (`used_memory_vm_functions` after 50,000
  `FUNCTION LOAD`s), `hot2.sh` (scripts/perf GET/SET rows).
- `res/r1`: 1.2.2, master `0e94bdd8`, each step and Redis 8.10.2;
  `res/r5`, `res/r6`: master `16da886f` and the branch merged with it and
  with `ea84fc08`; `res/cgq`, `res/cgr`, `res/r1cg`: callgrind; `res/hop`;
  `res/fuzz`; `res/repl`; `res/hot2`, `res/hot3`: scripts/perf; Redis suite
  summaries.
