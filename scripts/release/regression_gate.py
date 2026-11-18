#!/usr/bin/env python3
"""The release gate's logic (scripts/release/check-regression-results.sh runs
it; benchmarks/*/regressions/lib/test_compare.py tests it).

A stable tag needs committed regression-suite results that, together, cover
every suite in the judged tree's suites/ with a passing run:

- the full run (FULL=1, full scale, all engines, no --only) on the baseline's
  machine (Standard_D4s_v5). Suites that declare `Suite(..., min_cpus=N)`
  with N above that machine's CPUs are not run there (env.json
  `needs_more_cpus`), and their metrics are "not run": allowed for those
  suites only;
- for those suites, a `run-all.sh --min-cpus-only` run (FULL=1, full scale,
  all engines) on a machine with at least N CPUs (Standard_D8s_v5 for 8).

Every counted run needs verdict "ok", no failed gate, no failure, no
configuration skipped for want of --candidate-full, no suite run below its
min_cpus, and a candidate commit that is an ancestor of the judged commit
with src/, Cargo.toml, Cargo.lock and rust-toolchain.toml unchanged since
(the package version line aside).
"""
import json
import os
import re
import subprocess
import sys

IN_CI = os.environ.get("GITHUB_ACTIONS") == "true"
WATCHED = ["src", "Cargo.toml", "Cargo.lock", "rust-toolchain.toml"]


def say(msg):
    print(msg, flush=True)


def error(msg):
    say(("::error::" if IN_CI else "ERROR: ") + msg)


def suite_of(key):
    return key.split("/", 1)[0]


def judge(env, summ, suites, code_reasons=lambda env: []):
    """(reasons this run does not count, the suites it covers if it counts).

    code_reasons(env) gives the candidate-commit reasons (git); the rest is
    the run's own files."""
    why = []
    if summ.get("verdict") != "ok":
        why.append("verdict is %r, not 'ok'" % summ.get("verdict"))
    if summ.get("failed") or summ.get("problems"):
        why.append("summary lists failed gates or problems")
    if env.get("run_failed") or env.get("failures"):
        why.append("env.json records failures")
    skipped = env.get("skipped_configs") or []
    if skipped:
        why.append("configurations skipped (run-all.sh needs --candidate-full <lua,tls build> for them): %s"
                   % ", ".join(skipped))
    below = env.get("ran_below_min_cpus") or {}
    if below:
        why.append("suites run on fewer CPUs than they declare (not valid for the gate): %s" % ", ".join(
            "%s needs %s" % kv for kv in sorted(below.items())))
    # Absent is a refusal, not a default: a run from an older run-all.sh
    # cannot show it was a full run.
    missing = [k for k in ("scale", "only", "engines_selected", "full") if k not in env]
    min_only = bool(env.get("min_cpus_only"))
    if missing:
        why.append("env.json lacks %s (run it again with the current run-all.sh)" % ", ".join(missing))
    else:
        if str(env["scale"]) not in ("1", "1.0"):
            why.append("SCALE=%s: a smoke run" % env["scale"])
        if env["only"]:
            why.append("--only %s: a partial run" % env["only"])
        if not {"v1_2_2", "candidate", "redis"} <= set(env["engines_selected"]):
            why.append("not all three engines ran (%s)" % ",".join(env["engines_selected"]))
        if str(env["full"]) not in ("1", "true"):
            why.append("FULL=%s: the long configurations were not run" % env["full"])
    ran = set((env.get("suite_seconds") or {}).keys())
    needs_more = env.get("needs_more_cpus") or {}
    declared = dict(env.get("suite_min_cpus") or {})
    nproc = int(env.get("nproc") or 0)
    not_run = summ.get("not_run") or []
    if min_only:
        # Only the suites that declare min_cpus, each on a machine that big.
        covers = {s for s in ran if s in declared and int(declared[s]) <= nproc}
    else:
        covers = set(ran)
        bad = sorted({suite_of(k) for k in not_run} - set(needs_more))
        if bad:
            why.append("%d recorded metrics were not run (%s): a run without FULL=1 or with --only, or a "
                       "skipped configuration" % (len([k for k in not_run if suite_of(k) in bad]), ", ".join(bad)))
        unexplained = sorted(set(suites) - ran - set(needs_more))
        if unexplained:
            why.append("suites not run: %s" % ", ".join(unexplained))
    why += code_reasons(env)
    return why, (set() if why else covers & set(suites))


def coverage(suites, judged):
    """judged: {label: (why, covers)}. -> (labels used, suites no run covers)."""
    used, covered = [], set()
    for label, (why, covers) in sorted(judged.items()):
        if not why and covers - covered:
            used.append(label)
            covered |= covers
    return used, sorted(set(suites) - covered)


def needs(judged_envs):
    """The CPUs each declaring suite needs, from the runs' env.json."""
    out = {}
    for env in judged_envs:
        for s, n in list((env.get("suite_min_cpus") or {}).items()) + list((env.get("needs_more_cpus") or {}).items()):
            out[s] = max(out.get(s, 0), int(n))
    return out


def main(argv):
    tag, ref, repo, only_runs = None, None, ".", None
    args = list(argv)
    while args:
        a = args.pop(0)
        if a == "--ref" and args:
            ref = args.pop(0)
        elif a == "--repo" and args:
            repo = args.pop(0)
        elif a == "--runs" and args:
            only_runs = [r for r in args.pop(0).split(",") if r]
        elif a.startswith("-") or tag:
            say("usage: check-regression-results.sh <tag> [--ref <git-ref>] [--repo <dir>] [--runs a,b]")
            return 2
        else:
            tag = a
    if not tag:
        say("usage: check-regression-results.sh <tag> [--ref <git-ref>] [--repo <dir>] [--runs a,b]")
        return 2
    ref = ref or tag

    def git(*a, check=True):
        p = subprocess.run(["git", "-C", repo, *a], capture_output=True, text=True)
        if check and p.returncode != 0:
            raise RuntimeError("git %s failed: %s" % (" ".join(a), p.stderr.strip()))
        return p

    def show(rev, path):
        p = git("show", "%s:%s" % (rev, path), check=False)
        return p.stdout if p.returncode == 0 else None

    def ls(rev, path, dirs_only=False):
        a = ["ls-tree", "--name-only"] + (["-d"] if dirs_only else []) + ["%s:%s" % (rev, path)]
        p = git(*a, check=False)
        return p.stdout.split("\n")[:-1] if p.returncode == 0 else None

    m = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)(-.+)?", tag)
    if not m:
        error("%r is not a version tag (vX.Y.Z)" % tag)
        return 2
    if m.group(4):
        say("%s is a release candidate: the regression gate applies to stable tags only." % tag)
        return 0
    major, minor = m.group(1), m.group(2)
    head = git("rev-parse", "--verify", "--quiet", ref + "^{commit}", check=False).stdout.strip()
    if not head:
        error("cannot resolve %r to a commit in %s" % (ref, repo))
        return 2
    base = "benchmarks/%s.%s.0/regressions" % (major, minor)
    if ls(head, base) is None:
        say("No regression suite for %s.%s (%s is not in %s): nothing to check." % (major, minor, base, ref))
        return 0
    suites = sorted(n[:-3] for n in (ls(head, base + "/suites") or []) if re.fullmatch(r"\d\d-.*\.py", n))
    runs = sorted(ls(head, base + "/results", dirs_only=True) or [])
    if only_runs is not None:
        absent = [r for r in only_runs if r not in runs]
        if absent:
            error("--runs names runs that are not committed under %s/results: %s" % (base, ", ".join(absent)))
            return 1
        runs = only_runs
    say("Regression gate for %s: %s has %d suites and %d runs to judge; judging %s (%s)." % (
        tag, base, len(suites), len(runs), ref, head[:12]))
    if not suites:
        error("%s/suites has no suite" % base)
        return 1
    if not runs:
        error("%s/results has no committed run. Run the suite on the final release candidate and commit "
              "results/<label>/ (docs/RELEASING.md, \"Before tagging\")." % base)
        return 1

    def strip_version(path, text):
        out, in_block, done = [], False, False
        for line in text.split("\n"):
            s = line.strip()
            if path == "Cargo.toml":
                if s.startswith("["):
                    in_block = s == "[package]"
            else:
                if s == "[[package]]":
                    in_block = False
                elif s == 'name = "kividb"':
                    in_block = True
            if in_block and not done and re.match(r'version\s*=', s):
                done = True
                continue
            out.append(line)
        return "\n".join(out)

    def code_reasons(env):
        cand = (env.get("candidate_commit") or "").strip()
        c = git("rev-parse", "--verify", "--quiet", cand + "^{commit}", check=False).stdout.strip() \
            if re.fullmatch(r"[0-9a-fA-F]{7,40}", cand) else ""
        if not c:
            return ["candidate_commit %r is not a commit of this repository (pass --candidate-commit to "
                    "run-all.sh)" % cand]
        if git("merge-base", "--is-ancestor", c, head, check=False).returncode != 0:
            return ["candidate commit %s is not an ancestor of %s" % (c[:12], head[:12])]
        changed = []
        for n in filter(None, git("diff", "--name-only", c, head, "--", *WATCHED).stdout.split("\n")):
            if n in ("Cargo.toml", "Cargo.lock"):
                a, b = show(c, n), show(head, n)
                if a is not None and b is not None and strip_version(n, a) == strip_version(n, b):
                    continue
            changed.append(n)
        if changed:
            return ["code changed since the candidate commit %s: %s" % (
                c[:12], ", ".join(changed[:6]) + (" ..." if len(changed) > 6 else ""))]
        return []

    judged, envs = {}, []
    for run in runs:
        d = "%s/results/%s" % (base, run)
        raw_env, raw_sum = show(head, d + "/env.json"), show(head, d + "/summary.json")
        if raw_env is None or raw_sum is None:
            judged[run] = (["env.json or summary.json is missing"], set())
            continue
        try:
            env, summ = json.loads(raw_env), json.loads(raw_sum)
        except ValueError as e:
            judged[run] = (["env.json or summary.json is not valid JSON (%s)" % e], set())
            continue
        envs.append(env)
        judged[run] = judge(env, summ, suites, code_reasons)
    used, uncovered = coverage(suites, judged)
    if not uncovered:
        say("OK: %s together cover every suite (candidate commit matches the code being released)." % (
            ", ".join("%s/results/%s" % (base, u) for u in used)))
        return 0
    for run, (why, covers) in sorted(judged.items()):
        if why:
            say("  %s: %s" % (run, "; ".join(why)))
        else:
            say("  %s: counts, for %d suites" % (run, len(covers)))
    need = needs(envs)
    named = ", ".join("%s (needs a %s-CPU run: run-all.sh --min-cpus-only)" % (s, need[s]) if s in need else s
                      for s in uncovered)
    error("No passing run in %s/results covers: %s. Run the regression suite on the final release candidate "
          "(.github/workflows/regression-suite.yml: the Standard_D4s_v5 run and, for suites that declare "
          "min_cpus, the Standard_D8s_v5 run), commit its results/<label>/ and tag from that commit "
          "(docs/RELEASING.md, \"Before tagging\")." % (base, named))
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
