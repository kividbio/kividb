#!/usr/bin/env bash
# On the VM: step 1 (integration + lib tests, both I/O models), then the expiry_propagation x20 loop
# with a diagnostic print in debug_set_active_expire_0_stops_the_sweep, then step 2 (Tcl units).
set -u
. ~/.cargo/env
ulimit -n 65535
export CARGO_PROFILE_RELEASE_LTO=off CARGO_TARGET_DIR=~/tgt-full
R=~/res; mkdir -p $R
cd ~/src
TESTS="expiry_propagation expired_propagation expired_overwrite expire_bounds eviction maxmemory field_expiry_ms field_expiry_propagation hash_field_expiry replication binary_keys backup"
for io in uring tokio; do
  for t in $TESTS; do
    s=$(date +%s)
    KIVIDB_TEST_IO_MODEL=$io cargo test --release --features lua,tls --test $t --no-fail-fast > $R/it-$io-$t.log 2>&1 < /dev/null
    echo "$io $t exit=$? $(grep -E '^test result' $R/it-$io-$t.log | tail -1) $(( $(date +%s) - s ))s" | tee -a $R/summary-tests.txt
  done
done
s=$(date +%s)
cargo test --release --features lua,tls --lib -- store:: active_expiry expiry eviction ttl > $R/lib.log 2>&1 < /dev/null
echo "lib exit=$? $(grep -E '^test result' $R/lib.log | tail -1) $(( $(date +%s) - s ))s" | tee -a $R/summary-tests.txt

# Diagnostic (test-only, prints only on failure): what each wait saw on its last polls.
python3 - <<'EOF'
p='tests/expiry_propagation.rs'; s=open(p).read()
a='''    wait_until("k:0's DEL in the AOF", 10, || {
        deletions(&server.aof(), "k:0") == 1
    });'''
b='''    wait_until("k:0's DEL in the AOF", 10, || {
        let n = deletions(&server.aof(), "k:0");
        eprintln!("DIAG poll k:0 deletions in AOF = {n}");
        n == 1
    });'''
assert s.count(a)==1; s=s.replace(a,b)
a='''    wait_until("the sweep to delete the rest", 10, || {
        c.cmd(&["DBSIZE"]).i() == 0
            && c.info("stats", "expired_keys_active") == "4"
            && c.info("stats", "expired_subkeys") == "1"
    });'''
b='''    wait_until("the sweep to delete the rest", 10, || {
        let d = c.cmd(&["DBSIZE"]).i();
        let a = c.info("stats", "expired_keys_active");
        let s = c.info("stats", "expired_subkeys");
        let e = c.info("stats", "expired_keys");
        eprintln!("DIAG poll dbsize={d} expired_keys_active={a} expired_subkeys={s} expired_keys={e}");
        d == 0 && a == "4" && s == "1"
    });'''
assert s.count(a)==1; s=s.replace(a,b)
open(p,'w').write(s)
EOF
cargo test --release --features lua,tls --test expiry_propagation --no-run > $R/diag-build.log 2>&1
for io in uring tokio; do
  for i in $(seq 1 20); do
    KIVIDB_TEST_IO_MODEL=$io cargo test --release --features lua,tls --test expiry_propagation --no-fail-fast > $R/ep20-$io-$i.log 2>&1 < /dev/null
    echo "ep20 $io run $i exit=$? $(grep -E '^test result' $R/ep20-$io-$i.log | tail -1)" | tee -a $R/summary-ep20.txt
  done
done
git checkout tests/expiry_propagation.rs
echo TESTS-DONE | tee -a $R/summary-tests.txt

# Step 2: Tcl units, per unit, each I/O model.
UNITS="unit/expire unit/type/hash-field-expire unit/maxmemory"
for f in ~/redis-tests/tests/integration/replication*.tcl; do UNITS="$UNITS integration/$(basename $f .tcl)"; done
echo "units: $UNITS" > $R/tcl-units.txt
for io in uring tokio; do
  pkill -9 -x kividb-full 2>/dev/null; pkill -9 -x kividb 2>/dev/null; sleep 1
  d=$(mktemp -d); (cd $d && timeout 5 ~/bin/kividb-full --port 7399 --io-model $io --loglevel notice > $R/tcl-$io.iomodel.log 2>&1); rm -rf $d
  (cd ~/redis-tests && UNITS="$UNITS" PORT=6399 SERVER_ARGS="--io-model $io" bash ~/src/scripts/redis_compat.sh ~/bin/kividb-full ~/redis-tests $R/tcl-$io > $R/tcl-$io.console.log 2>&1)
done
echo TCL-DONE | tee -a $R/summary-tests.txt
