#!/usr/bin/env bash
# Release gate: a stable tag needs a committed regression-suite result for the
# code it ships. Bash and python3 only; reads the repository through git, so it
# needs no checkout of the tag and runs the same locally and in CI.
#
#   scripts/release/check-regression-results.sh <tag> [--ref <git-ref>] [--repo <dir>]
#
#   <tag>   vX.Y.Z (or X.Y.Z). A tag with a "-" (a release candidate) is not checked.
#   --ref   the commit whose tree is judged (default: <tag>). Locally, before
#           tagging, pass the release branch: --ref HEAD.
#   --repo  the git repository (default: the current directory).
#
# The suite of a minor release lives in benchmarks/X.Y.0/regressions/. If that
# directory does not exist in the tree (every release before the suite, and
# patch releases of a minor that never had one), nothing is checked.
# Otherwise results/ there must hold at least one run that
#
#   - has summary.json verdict "ok" (not REGRESSED, INVALID or SMOKE), no failed
#     gate and no failure in env.json;
#   - is a FULL=1, full-scale run of every suite in suites/ and every engine
#     (no SCALE, no --only, no --engines subset, no metric "not run"), and
#     env.json has the scale, only, engines_selected and full keys;
#   - names a candidate commit that exists, is an ancestor of the commit being
#     judged, and left src/, Cargo.toml, Cargo.lock and rust-toolchain.toml
#     alone since (the package version line aside: the release bumps it after
#     the candidate was measured).
#
# Exit 0: nothing to check, or a run passes. Exit 1: refused. Exit 2: usage.
set -euo pipefail

usage() { sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//' >&2; exit 2; }

TAG= REF= REPO=.
while [ $# -gt 0 ]; do
  case $1 in
    --ref) [ $# -ge 2 ] || usage; REF=$2; shift 2 ;;
    --repo) [ $# -ge 2 ] || usage; REPO=$2; shift 2 ;;
    -h | --help) usage ;;
    -*) echo "unknown argument $1" >&2; usage ;;
    *) [ -z "$TAG" ] || usage; TAG=$1; shift ;;
  esac
done
[ -n "$TAG" ] || usage
[ -n "$REF" ] || REF=$TAG

exec python3 - "$TAG" "$REF" "$REPO" <<'PY'
import json, os, re, subprocess, sys

tag, ref, repo = sys.argv[1:4]
IN_CI = os.environ.get("GITHUB_ACTIONS") == "true"


def say(msg):
    print(msg, flush=True)


def error(msg):
    say(("::error::" if IN_CI else "ERROR: ") + msg)


def git(*args, check=True):
    p = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True)
    if check and p.returncode != 0:
        raise RuntimeError("git %s failed: %s" % (" ".join(args), p.stderr.strip()))
    return p


def show(rev, path):
    p = git("show", "%s:%s" % (rev, path), check=False)
    return p.stdout if p.returncode == 0 else None


def ls(rev, path, dirs_only=False):
    args = ["ls-tree", "--name-only"] + (["-d"] if dirs_only else []) + ["%s:%s" % (rev, path)]
    p = git(*args, check=False)
    return p.stdout.split("\n")[:-1] if p.returncode == 0 else None


m = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)(-.+)?", tag)
if not m:
    error("%r is not a version tag (vX.Y.Z)" % tag)
    sys.exit(2)
if m.group(4):
    say("%s is a release candidate: the regression gate applies to stable tags only." % tag)
    sys.exit(0)
major, minor = m.group(1), m.group(2)

head = git("rev-parse", "--verify", "--quiet", ref + "^{commit}", check=False).stdout.strip()
if not head:
    error("cannot resolve %r to a commit in %s" % (ref, repo))
    sys.exit(2)

base = "benchmarks/%s.%s.0/regressions" % (major, minor)
if ls(head, base) is None:
    say("No regression suite for %s.%s (%s is not in %s): nothing to check." % (major, minor, base, ref))
    sys.exit(0)

suites = sorted(n[:-3] for n in (ls(head, base + "/suites") or []) if re.fullmatch(r"\d\d-.*\.py", n))
runs = sorted(ls(head, base + "/results", dirs_only=True) or [])
say("Regression gate for %s: %s has %d suites and %d committed runs; judging %s (%s)." % (
    tag, base, len(suites), len(runs), ref, head[:12]))
if not suites:
    error("%s/suites has no suite" % base)
    sys.exit(1)

WATCHED = ["src", "Cargo.toml", "Cargo.lock", "rust-toolchain.toml"]


def strip_version(path, text):
    """Drop the kividb package's own version line (a release bumps it)."""
    out, in_block, done = [], False, False
    for line in text.split("\n"):
        s = line.strip()
        if path == "Cargo.toml":
            if s.startswith("["):
                in_block = s == "[package]"
        else:  # Cargo.lock
            if s == "[[package]]":
                in_block = False
            elif s == 'name = "kividb"':
                in_block = True
        if in_block and not done and re.match(r'version\s*=', s):
            done = True
            continue
        out.append(line)
    return "\n".join(out)


def code_changes(cand):
    """Files of WATCHED that differ between the candidate and the judged commit."""
    names = git("diff", "--name-only", cand, head, "--", *WATCHED).stdout.split("\n")
    changed = []
    for n in filter(None, names):
        if n in ("Cargo.toml", "Cargo.lock"):
            a, b = show(cand, n), show(head, n)
            if a is not None and b is not None and strip_version(n, a) == strip_version(n, b):
                continue
        changed.append(n)
    return changed


def judge(run):
    """The reasons this run does not count (empty: it counts)."""
    d = "%s/results/%s" % (base, run)
    raw_env, raw_sum = show(head, d + "/env.json"), show(head, d + "/summary.json")
    if raw_env is None or raw_sum is None:
        return ["env.json or summary.json is missing"]
    try:
        env, summ = json.loads(raw_env), json.loads(raw_sum)
    except ValueError as e:
        return ["env.json or summary.json is not valid JSON (%s)" % e]
    why = []
    if summ.get("verdict") != "ok":
        why.append("verdict is %r, not 'ok'" % summ.get("verdict"))
    if summ.get("failed") or summ.get("problems"):
        why.append("summary lists failed gates or problems")
    if summ.get("not_run"):
        why.append("%d recorded metrics were not run (a run without FULL=1 or with --only)" % len(summ["not_run"]))
    if env.get("run_failed") or env.get("failures"):
        why.append("env.json records failures")
    # Absent is a refusal, not a default: a run from an older run-all.sh
    # cannot show it was a full run.
    missing = [k for k in ("scale", "only", "engines_selected", "full") if k not in env]
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
    if not set(suites) <= ran:
        why.append("suites not run: %s" % ", ".join(sorted(set(suites) - ran)))
    cand = (env.get("candidate_commit") or "").strip()
    c = git("rev-parse", "--verify", "--quiet", cand + "^{commit}", check=False).stdout.strip() if re.fullmatch(r"[0-9a-fA-F]{7,40}", cand) else ""
    if not c:
        why.append("candidate_commit %r is not a commit of this repository (pass --candidate-commit to run-all.sh)" % cand)
    elif git("merge-base", "--is-ancestor", c, head, check=False).returncode != 0:
        why.append("candidate commit %s is not an ancestor of %s" % (c[:12], head[:12]))
    else:
        changed = code_changes(c)
        if changed:
            why.append("%s changed since the candidate commit %s: %s" % (
                "code", c[:12], ", ".join(changed[:6]) + (" ..." if len(changed) > 6 else "")))
    return why


if not runs:
    error("%s/results has no committed run. Run the suite on the final release candidate and commit results/<label>/ (docs/RELEASING.md, \"Before tagging\")." % base)
    sys.exit(1)

for run in runs:
    why = judge(run)
    if not why:
        say("OK: %s/results/%s passes (candidate commit matches the code being released)." % (base, run))
        sys.exit(0)
    say("  %s: %s" % (run, "; ".join(why)))

error("No run in %s/results counts for %s. Run the regression suite on the final release candidate "
      "(.github/workflows/regression-suite.yml), commit its results/<label>/ and tag from that commit "
      "(docs/RELEASING.md, \"Before tagging\")." % (base, tag))
sys.exit(1)
PY
