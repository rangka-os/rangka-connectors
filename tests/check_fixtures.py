#!/usr/bin/env python3
"""Check a connector's spec against its synthetic fixtures (no network, no account).

    python3 tests/check_fixtures.py [connector ...]     # default: every <name>/fixtures/ in this repo

Needs PyYAML (pip install pyyaml), as the rangka-os validator does.

For a connector with a fixtures/ folder:
- every trigger has at least one fixture (<trigger>.json or <trigger>_<case>.json), every action with an output one
  (<action>.json, the answer of the API);
- every emit / output that is a field path resolves in each of its fixtures (null is allowed: the field is there);
- an emit that is an expression (comparisons, ||, &&, a ? b : c, text methods, list indexes) is evaluated on the
  fixture;
- fixtures/expect.json pins values per fixture ({"<file>": {"<emit>": value}}); a field pinned to null may be absent;
- every {input.x}, {auth.x} and settings.x the spec reads is declared (in requests and in a mail connector's mail:);
- a mail: fetch answer (rangka-os contract 0068) has messages and a cursor, each message every field its trigger emits.
This keeps a spec honest about the payloads it was written from, before the kernel sends a real request.
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


TOKEN = re.compile(r"\s*(?:(?P<str>'[^']*')|(?P<num>\d+(?:\.\d+)?)|(?P<op>==|!=|>=|<=|\|\||&&|[!?:()\[\],.<>+])|(?P<id>[A-Za-z_][A-Za-z0-9_]*))")


def evaluate(expr, doc):
    """A CEL subset, enough for emit and output: field paths (body and response are the payload itself), 'strings',
    numbers, true/false/null, == != > < >= <=, !, || and && (short-circuit), a ? b : c, 'key' in path, x in [list],
    list[i], size(x), + on texts, and the text methods endsWith, startsWith, contains and replace.

    A path that is not in the fixture fails only if it is evaluated, as in CEL."""
    toks, pos = [], 0
    while pos < len(expr):
        m = TOKEN.match(expr, pos)
        if not m or m.end() == pos:
            if expr[pos:].strip() == "":
                break
            raise ValueError(f"outside the checked subset: {expr}")
        pos = m.end()
        toks.append((m.lastgroup, m.group(m.lastgroup)))
    toks.append(("end", ""))
    i = 0

    def peek():
        return toks[i][1] if toks[i][0] in ("op", "id") else None

    def take(want=None):
        nonlocal i
        kind, val = toks[i]
        if want is not None and (kind not in ("op", "id") or val != want):
            raise ValueError(f"outside the checked subset: {expr}")
        i += 1
        return kind, val

    # every node is a thunk, so that only the evaluated branch reads the fixture
    def ternary():
        c = or_()
        if peek() == "?":
            take()
            a = ternary()
            take(":")
            b = ternary()
            return lambda: a() if c() else b()
        return c

    def or_():
        left = and_()
        while peek() == "||":
            take()
            l, r = left, and_()
            left = (lambda l, r: lambda: bool(l()) or bool(r()))(l, r)
        return left

    def and_():
        left = not_()
        while peek() == "&&":
            take()
            l, r = left, not_()
            left = (lambda l, r: lambda: bool(l()) and bool(r()))(l, r)
        return left

    def not_():
        if peek() == "!":
            take()
            x = not_()
            return lambda: not x()
        return cmp_()

    def cmp_():
        left = add()
        op = peek()
        if op in ("==", "!=", ">", "<", ">=", "<=", "in"):
            take()
            right = add()
            return {"==": lambda: left() == right(), "!=": lambda: left() != right(), ">": lambda: left() > right(),
                    "<": lambda: left() < right(), ">=": lambda: left() >= right(), "<=": lambda: left() <= right(),
                    "in": lambda: left() in right()}[op]
        return left

    def add():
        left = postfix()
        while peek() == "+":
            take()
            l, r = left, postfix()
            left = (lambda l, r: lambda: l() + r())(l, r)
        return left

    def args():
        out = []
        take("(")
        while peek() != ")":
            out.append(ternary())
            if peek() == ",":
                take()
        take(")")
        return out

    def postfix():
        kind, val = take()
        if kind == "str":
            node = (lambda v: lambda: v)(val[1:-1])
        elif kind == "num":
            node = (lambda v: lambda: v)(float(val) if "." in val else int(val))
        elif kind == "op" and val == "(":
            node = ternary()
            take(")")
        elif kind == "op" and val == "[":
            items = []
            while peek() != "]":
                items.append(ternary())
                if peek() == ",":
                    take()
            take("]")
            node = lambda: [x() for x in items]
        elif kind == "id" and val in ("true", "false", "null"):
            node = (lambda v: lambda: v)({"true": True, "false": False, "null": None}[val])
        elif kind == "id" and val == "size" and peek() == "(":
            a = args()
            node = lambda: len(a[0]())
        elif kind == "id":
            def root(name=val):
                if name in ("body", "response"):
                    return doc
                if name not in doc:
                    raise KeyError(name)
                return doc[name]
            node = root
        else:
            raise ValueError(f"outside the checked subset: {expr}")
        while True:
            if peek() == ".":
                take()
                _, name = take()
                if peek() == "(":
                    a, recv = args(), node
                    if name not in ("endsWith", "startsWith", "contains", "replace"):
                        raise ValueError(f"outside the checked subset: {expr}")
                    node = (lambda recv, name, a: lambda: {
                        "endsWith": lambda s: s.endswith(a[0]()), "startsWith": lambda s: s.startswith(a[0]()),
                        "contains": lambda s: a[0]() in s, "replace": lambda s: s.replace(a[0](), a[1]())}[name](recv()))(recv, name, a)
                else:
                    def field(recv=node, name=name):
                        v = recv()
                        if not isinstance(v, dict) or name not in v:
                            raise KeyError(name)
                        return v[name]
                    node = field
            elif peek() == "[":
                take()
                idx, recv = ternary(), node
                take("]")
                node = (lambda recv, idx: lambda: recv()[idx()])(recv, idx)
            else:
                return node

    tree = ternary()
    if toks[i][0] != "end":
        raise ValueError(f"outside the checked subset: {expr}")
    return tree()


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
                    # a field a payload may leave out is pinned to null in expect.json (the kernel emits null)
                    if key in expect.get(base, {}) and expect[base][key] is None:
                        got[key] = None
                    else:
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
    # a mail connector (rangka-os contract 0068): the mail: block reads declared auth fields only, and the answer of a
    # mail: fetch action (<action>.json) holds messages and a cursor, each message with every field the poll trigger
    # that sends it emits from item
    for x in sorted(set(re.findall(r"auth\.([a-z_][a-z0-9_]*)", json.dumps(spec.get("mail") or {}))) - auth):
        errs.append(f"mail: auth.{x} is not declared")
    for a in spec.get("actions", []):
        if a.get("mail") != "fetch":
            continue
        for f in sorted(glob.glob(os.path.join(fx, a["name"] + ".json"))):
            doc = json.load(open(f))
            n += 1
            if not isinstance(doc.get("messages"), list) or not isinstance(doc.get("cursor"), str):
                errs.append(f"{os.path.basename(f)}: a fetch answer has messages (a list) and cursor (a text)")
                continue
            for t in spec.get("triggers", []):
                if t.get("action") != a["name"]:
                    continue
                for i, m in enumerate(doc["messages"]):
                    for key, val in (t.get("emit") or {}).items():
                        n += 1
                        if PATH.match(str(val)) and str(val).startswith("item.") and resolve({"item": m}, str(val)) is MISSING:
                            errs.append(f"{os.path.basename(f)}: messages[{i}] has no {str(val)[5:]} (emit {key})")
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
