"""Generate src/commands/command_docs_table.rs from Redis command JSON.

usage: python -I gen_command_docs.py <redis src/commands dir> <redis version> <out.rs> [extra.json ...]

Each extra path is a Redis-style commands.json (one object mapping command
names to specs), or a directory of such files. Later files override earlier
ones of the same full name. Redis 8.10.2's `src/commands/*.json` is the
base; extras fill in bundled modules (JSON, search, time series, vector
sets) and the few commands KiviDB has that those trees do not.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

KNOWN_ARG_TYPES = {
    "string",
    "integer",
    "double",
    "key",
    "pattern",
    "unix-time",
    "pure-token",
    "oneof",
    "block",
}

# JSON `group` -> COMMAND DOCS `group`, as Redis's COMMAND_GROUP_STR.
GROUP_REPLY = {
    "generic": "generic",
    "string": "string",
    "list": "list",
    "set": "set",
    "sorted_set": "sorted-set",
    "hash": "hash",
    "pubsub": "pubsub",
    "transactions": "transactions",
    "connection": "connection",
    "server": "server",
    "scripting": "scripting",
    "hyperloglog": "hyperloglog",
    "cluster": "cluster",
    "sentinel": "sentinel",
    "geo": "geo",
    "stream": "stream",
    "bitmap": "bitmap",
    "array": "array",
    "module": "module",
    "json": "json",
    "search": "search",
    "timeseries": "timeseries",
    "vector_set": "vectorset",
    "vectorset": "vectorset",
}

DOC_FLAGS = {"DEPRECATED": "deprecated", "SYSCMD": "syscmd"}


def rust_str(s: str) -> str:
    return json.dumps(s, ensure_ascii=False)


def rust_opt(s: str | None) -> str:
    return "None" if s is None else f"Some({rust_str(s)})"


def group_reply(group: str | None) -> str:
    if not group:
        return "server"
    if group in GROUP_REPLY:
        return GROUP_REPLY[group]
    return group.replace("_", "-")


def load_json_files(path: Path) -> list[tuple[str, dict]]:
    if path.is_dir():
        files = sorted(path.glob("*.json"))
    else:
        files = [path]
    out = []
    for file in files:
        doc = json.loads(file.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue
        for name, spec in doc.items():
            if isinstance(spec, dict):
                out.append((name, spec))
    return out


def full_name(name: str, spec: dict) -> str:
    name = name.upper()
    if "container" in spec:
        return f"{spec['container'].upper()}|{name}"
    if " " in name:
        container, sub = name.split(" ", 1)
        return f"{container}|{sub.replace(' ', '|')}"
    return name


def split_space_name(name: str, spec: dict) -> tuple[str, dict]:
    """`FT.CONFIG GET` in a module JSON is the subcommand GET of FT.CONFIG."""
    if "container" in spec or " " not in name:
        return name, spec
    container, sub = name.split(" ", 1)
    spec = dict(spec)
    spec.setdefault("container", container)
    return sub, spec


def normalize_arg(desc: dict) -> dict | None:
    desc = dict(desc)
    if "name" not in desc:
        token = desc.get("token")
        if not token:
            return None
        desc["name"] = str(token).lower()
        desc.setdefault("type", "pure-token")
    name = str(desc["name"]).lower()
    if "_" in name:
        name = name.replace("_", "-")
    desc["name"] = name
    typ = desc.get("type")
    if typ not in KNOWN_ARG_TYPES:
        if desc.get("arguments"):
            typ = "block"
        elif desc.get("token") and typ in (None, "function"):
            typ = "pure-token"
        else:
            typ = "string"
        desc["type"] = typ
    if typ in ("oneof", "block"):
        desc["display"] = None
        nested = []
        for sub in desc.get("arguments") or []:
            n = normalize_arg(sub)
            if n:
                nested.append(n)
        desc["arguments"] = nested
    else:
        if "display" in desc and desc["display"] is not None:
            desc["display"] = str(desc["display"]).lower()
        desc["arguments"] = []
    if desc.get("token"):
        desc["token"] = str(desc["token"]).upper()
    return desc


def arg_flags(desc: dict) -> list[str]:
    flags = []
    if desc.get("optional"):
        flags.append("optional")
    if desc.get("multiple"):
        flags.append("multiple")
    if desc.get("multiple_token"):
        flags.append("multiple_token")
    return flags


def emit_arg(desc: dict, indent: str) -> str:
    nested = desc.get("arguments") or []
    if nested:
        inner = ",\n".join(emit_arg(a, indent + "    ") for a in nested)
        args = f"&[\n{inner},\n{indent}    ]"
    else:
        args = "&[]"
    flags = arg_flags(desc)
    flags_rs = "&[" + ", ".join(rust_str(f) for f in flags) + "]" if flags else "&[]"
    display = None
    if desc["type"] not in ("oneof", "block"):
        display = desc.get("display") or desc["name"]
    ksi = desc.get("key_spec_index")
    if ksi is None:
        ksi = -1
    fields = [
        f"name: {rust_str(desc['name'])}",
        f"type_: {rust_str(desc['type'])}",
        f"display_text: {rust_opt(display)}",
        f"key_spec_index: {int(ksi)}",
        f"token: {rust_opt(desc.get('token'))}",
        f"summary: {rust_opt(desc.get('summary'))}",
        f"since: {rust_opt(desc.get('since'))}",
        f"deprecated_since: {rust_opt(desc.get('deprecated_since'))}",
        f"flags: {flags_rs}",
        f"arguments: {args}",
    ]
    pad = ",\n".join(f"{indent}    {f}" for f in fields)
    return f"{indent}Arg {{\n{pad},\n{indent}}}"


def emit_docs(spec: dict, sub_names: list[str], indent: str) -> str:
    history = spec.get("history") or []
    if history:
        items = ",\n".join(
            f"{indent}        History({rust_str(h[0])}, {rust_str(h[1])})" for h in history
        )
        hist_rs = f"&[\n{items},\n{indent}    ]"
    else:
        hist_rs = "&[]"
    doc_flags = [DOC_FLAGS[f] for f in spec.get("doc_flags") or [] if f in DOC_FLAGS]
    flags_rs = "&[" + ", ".join(rust_str(f) for f in doc_flags) + "]" if doc_flags else "&[]"
    args = [normalize_arg(a) for a in spec.get("arguments") or []]
    args = [a for a in args if a]
    if args:
        inner = ",\n".join(emit_arg(a, indent + "    ") for a in args)
        args_rs = f"&[\n{inner},\n{indent}    ]"
    else:
        args_rs = "&[]"
    if sub_names:
        subs_rs = "&[" + ", ".join(rust_str(n) for n in sub_names) + "]"
    else:
        subs_rs = "&[]"
    fields = [
        f"summary: {rust_str(spec.get('summary') or '')}",
        f"since: {rust_str(spec.get('since') or '')}",
        f"group: {rust_str(group_reply(spec.get('group')))}",
        f"complexity: {rust_str(spec.get('complexity') or '')}",
        f"doc_flags: {flags_rs}",
        f"deprecated_since: {rust_opt(spec.get('deprecated_since'))}",
        f"replaced_by: {rust_opt(spec.get('replaced_by'))}",
        f"history: {hist_rs}",
        f"arguments: {args_rs}",
        f"subcommands: {subs_rs}",
    ]
    pad = ",\n".join(f"{indent}    {f}" for f in fields)
    return f"Docs {{\n{pad},\n{indent}}}"


def main() -> None:
    if len(sys.argv) < 4:
        print(__doc__.strip(), file=sys.stderr)
        sys.exit(2)
    src, version, out = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
    extras = [Path(p) for p in sys.argv[4:]]

    entries: dict[str, dict] = {}
    children: dict[str, list[str]] = defaultdict(list)

    def ingest(pairs: list[tuple[str, dict]]) -> None:
        for name, spec in pairs:
            name, spec = split_space_name(name, spec)
            full = full_name(name, spec)
            entries[full] = spec
            if "container" in spec:
                children[spec["container"].upper()].append(full)

    ingest(load_json_files(src))
    for extra in extras:
        ingest(load_json_files(extra))

    # Module JSON often lists `FT.CONFIG GET` without a container object.
    for container, subs in list(children.items()):
        if container not in entries:
            first = entries[subs[0]]
            entries[container] = {
                "summary": f"A container for {container} commands.",
                "complexity": "Depends on subcommand.",
                "group": first.get("group", "server"),
                "since": min((entries[s].get("since") or "1.0.0") for s in subs),
                "arity": -2,
            }

    for name, spec in entries.items():
        if "|" not in name and name in children and "group" in spec:
            for sub in children[name]:
                entries[sub].setdefault("group", spec["group"])

    body = [
        f"// Generated from Redis {version} `src/commands/*.json` (and extra",
        "// command JSON passed to `gen_command_docs.py`). Not edited by hand.",
        "",
        "use super::command_docs::{Arg, Docs, History};",
        "",
        "/// `(command, docs)`, by command, including `CONTAINER|SUBCOMMAND`.",
        "#[rustfmt::skip]",
        "pub(super) const TABLE: &[(&str, Docs)] = &[",
    ]
    for name in sorted(entries):
        spec = entries[name]
        subs = sorted(set(children.get(name, [])))
        docs = emit_docs(spec, subs, "    ")
        body.append(f"    ({json.dumps(name)}, {docs}),")
    body.append("];")
    out.write_text("\n".join(body) + "\n", encoding="utf-8")
    print(len(entries), "commands")


if __name__ == "__main__":
    main()
