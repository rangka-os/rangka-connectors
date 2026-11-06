#!/usr/bin/env python3
"""Check a connector's spec against its synthetic fixtures (no network, no account).

    python3 tests/check_fixtures.py [connector ...]     # default: every <name>/fixtures/ in this repo

Needs PyYAML (pip install pyyaml), as the rangka-os validator does.

For a connector with a fixtures/ folder:
- every trigger has at least one fixture (<trigger>.json or <trigger>_<case>.json), every action with an output one
  (<action>.json, the answer of the API);
- every emit / output that is a field path resolves in each of its fixtures (null is allowed: the field is there);
- an emit that is an expression (==, ||, &&, string literals) is evaluated on the fixture;
- fixtures/expect.json pins values per fixture ({"<file>": {"<emit>": value}});
- every {input.x}, {auth.x} and settings.x the spec reads is declared.
The kernel does not run connectors yet (contract 0002); this keeps a spec honest about the payloads it was written from.
"""
import glob
import json
import os
import re
import sys

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PATH = re.compile(r"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)*$")
MISSING = object()


def resolve(doc, path):
    cur = doc
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return MISSING
        cur = cur[part]
    return cur


def evaluate(expr, doc):
    """A small CEL subset: paths, 'strings', true/false, ==, !=, ||, &&, parentheses."""
    tokens = re.findall(r"'[^']*'|==|!=|\|\||&&|\(|\)|[a-z_][a-z0-9_.]*", expr)
    if "".join(tokens).replace(" ", "") != expr.replace(" ", ""):
        raise ValueError(f"outside the checked subset: {expr}")
    out = []
    for t in tokens:
        if t.startswith("'"):
            out.append(repr(t[1:-1]))
        elif t in ("==", "!=", "(", ")"):
            out.append(t)
        elif t == "||":
            out.append(" or ")
        elif t == "&&":
            out.append(" and ")
        elif t in ("true", "false"):
            out.append(t.capitalize())
        else:
            v = resolve(doc, t)
            if v is MISSING:
                raise KeyError(t)
            out.append(repr(v))
    return eval(" ".join(out), {"__builtins__": {}})  # noqa: S307 - only the literals built above


def check(name):
    errs, n = [], 0
    spec = yaml.safe_load(open(os.path.join(ROOT, name, "connector.rk")))
    fx = os.path.join(ROOT, name, "fixtures")
    expect = json.load(open(os.path.join(fx, "expect.json"))) if os.path.exists(os.path.join(fx, "expect.json")) else {}
    auth = {f["name"] for f in (spec.get("auth") or {}).get("fields", [])}
    settings = {f["name"] for f in spec.get("settings", [])}

    def mapping(kind, unit, emits, files):
        nonlocal n
        if not files:
            errs.append(f"{kind} {unit}: no fixture")
        for f in files:
            doc = json.load(open(f))
            base = os.path.basename(f)
            got = {}
            for key, val in emits.items():
                n += 1
                val = str(val)
                try:
                    got[key] = resolve(doc, val) if PATH.match(val) else evaluate(val, doc)
                except (KeyError, ValueError) as e:
                    errs.append(f"{base}: {key} = {val}: {e}")
                    continue
                if got[key] is MISSING:
                    errs.append(f"{base}: {key} = {val}: no such field")
            for key, want in expect.get(base, {}).items():
                n += 1
                if key not in got:
                    errs.append(f"{base}: expect.json names {key}, which {unit} does not emit")
                elif got[key] != want:
                    errs.append(f"{base}: {key} = {got[key]!r}, want {want!r}")

    for t in spec.get("triggers", []):
        files = sorted(glob.glob(os.path.join(fx, t["name"] + ".json")) + glob.glob(os.path.join(fx, t["name"] + "_*.json")))
        mapping("trigger", t["name"], t.get("emit", {}), files)
    for a in spec.get("actions", []):
        inputs = {i["name"] for i in a.get("input", [])}
        text = json.dumps(a.get("request", {})) + " " + json.dumps(a.get("idempotency", "")) + " " + json.dumps([i.get("default", "") for i in a.get("input", [])])
        for kind, names, declared in (("input", re.findall(r"input\.([a-z_][a-z0-9_]*)", text), inputs),
                                      ("auth", re.findall(r"auth\.([a-z_][a-z0-9_]*)", text), auth),
                                      ("settings", re.findall(r"settings\.([a-z_][a-z0-9_]*)", text), settings)):
            for x in sorted(set(names) - declared):
                errs.append(f"action {a['name']}: {kind}.{x} is not declared")
        if a.get("output"):
            mapping("action", a["name"], a["output"], sorted(glob.glob(os.path.join(fx, a["name"] + ".json"))))
    for t in spec.get("triggers", []):
        for k, v in (t.get("verify") or {}).items():
            for x in re.findall(r"auth\.([a-z_][a-z0-9_]*)", str(v)):
                if x not in auth:
                    errs.append(f"trigger {t['name']}: verify reads auth.{x}, not declared")
    return n, errs


def main():
    names = sys.argv[1:] or sorted(os.path.basename(os.path.dirname(d)) for d in glob.glob(os.path.join(ROOT, "*", "fixtures")))
    bad = 0
    for name in names:
        n, errs = check(name)
        for e in errs:
            print(f"FAIL  {name}: {e}")
        print(f"{name}: {n} checks, {len(errs)} failed")
        bad += len(errs)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
