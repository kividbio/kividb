#!/usr/bin/env bash
# Keys are bytes, not text (#510): a command's key argument must reach the
# store as bytes, not through `from_utf8` or `to_str`. This greps
# `src/commands` for the conversions that would mean a regression where a
# key could be the argument — `parse_utf8` or a `from_utf8` applied to a
# `key`/`k`, or to the first argument, where most commands' key sits.
#
# Every match needs a line in scripts/key_utf8_allowlist.txt: the file,
# text the line contains, and one line of why the conversion is
# legitimate. A match the list does not know is new and fails the gate;
# a line that matches nothing is stale and fails it too.
set -euo pipefail
cd "$(dirname "$0")/.."

ALLOW="scripts/key_utf8_allowlist.txt"
hits="$(mktemp)"
used="$(mktemp)"
trap 'rm -f "$hits" "$used"' EXIT

grep -rnE '(parse_utf8|str::from_utf8|from_utf8_lossy|from_utf8_unchecked|\.to_str\(\))' \
    src/commands --include='*.rs' \
    | grep -vE ':[0-9]+:\s*//' \
    | grep -vE ':[0-9]+:\s*(pub\s+)?use ' \
    | grep -E '\b(key|k)\b|cmd\[\s*1\s*\]|cmd\.get\(\s*1\s*\)|args\[\s*1\s*\]' \
    > "$hits" || true

fail=0
while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    file="${hit%%:*}"
    text="${hit#*:}"
    text="${text#*:}"
    match=""
    while IFS='|' read -r afile asnip _areason; do
        case "$afile" in \#*|"") continue ;; esac
        [ "$afile" = "$file" ] || continue
        case "$text" in
            *"$asnip"*) match=1; echo "$afile|$asnip" >> "$used"; break ;;
        esac
    done < "$ALLOW"
    if [ -z "$match" ]; then
        echo "not in $ALLOW: $hit"
        fail=1
    fi
done < "$hits"

while IFS='|' read -r afile asnip areason; do
    case "$afile" in \#*|"") continue ;; esac
    if [ -z "${areason// /}" ]; then
        echo "$afile|$asnip has no reason"
        fail=1
    fi
    if ! grep -qxF "$afile|$asnip" "$used"; then
        echo "stale allow-list entry: $afile|$asnip"
        fail=1
    fi
done < "$ALLOW"

exit "$fail"
