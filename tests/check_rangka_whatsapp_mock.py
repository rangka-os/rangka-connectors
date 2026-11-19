#!/usr/bin/env python3
"""Check rangka_whatsapp/mock/rangka_whatsapp_mock.py against the connector's own manifest: the send_template and
set_opt_out requests are built from connector.rk (url, headers, body) and accepted, a key is sent once, an opted-out
customer and a stopped or paused service get nothing, only 429 and 5xx are answers the manifest retries, and every
event the mock posts verifies with the manifest's signature rule and carries every field its trigger emits.
Needs PyYAML (as tests/check_fixtures.py does); no network beyond 127.0.0.1.

    python3 tests/check_rangka_whatsapp_mock.py
"""
import hashlib
import hmac
import json
import os
import re
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "rangka_whatsapp", "mock"))
import rangka_whatsapp_mock as m  # noqa: E402

SPEC = yaml.safe_load(open(os.path.join(ROOT, "rangka_whatsapp", "connector.rk")))
TOKEN, HOOK_KEY = "dummy-wa-token-check", "dummy-wa-hook-check"
fails = []


def check(name, cond, detail=""):
    print(("ok    " if cond else "FAIL  ") + name + ("" if cond else f"  {detail}"))
    if not cond:
        fails.append(name)


got = []  # (trigger, event) the receiver accepted
refused = []


def verify(trigger, headers, raw):
    """The kernel's check, read from the manifest: header, prefix, alg and key field of verify.signature."""
    sig = next(t for t in SPEC["triggers"] if t["name"] == trigger)["verify"]["signature"]
    assert sig["alg"] == "hmac-sha256" and sig["key"] == "auth.webhook_key"
    want = sig.get("prefix", "") + hmac.new(HOOK_KEY.encode(), raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(headers.get(sig["header"], ""), want)


class Hook(BaseHTTPRequestHandler):
    """The receiver (a kernel's /hooks/<org>/rangka_whatsapp/<trigger>): accepts only a rightly signed event."""

    def log_message(self, *a):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        trigger = self.path.rsplit("/", 1)[-1]
        if not verify(trigger, self.headers, raw):
            refused.append(trigger)
            self.send_response(401)
            self.end_headers()
            return
        got.append((trigger, json.loads(raw)))
        self.send_response(200)
        self.end_headers()


def render(action, auth, inp):
    """Build an action's request from the manifest the way the kernel does: {auth.x} and {input.x} placeholders; a
    string that is exactly one placeholder keeps its value's type; a placeholder with no value drops the key."""
    act = next(a for a in SPEC["actions"] if a["name"] == action)
    for f in act["input"]:
        if f.get("required") and f["name"] not in inp:
            raise KeyError(f["name"])
    vals = {**{f"auth.{k}": v for k, v in auth.items()}, **{f"input.{k}": v for k, v in inp.items()}}

    def sub(v):
        if isinstance(v, str):
            one = re.fullmatch(r"\{([a-z_.]+)\}", v)
            if one:
                return vals.get(one.group(1), KeyError)
            return re.sub(r"\{([a-z_.]+)\}", lambda mm: str(vals[mm.group(1)]), v)
        return v

    req = act["request"]
    body = {k: sub(v) for k, v in req["body"].items()}
    body = {k: v for k, v in body.items() if v is not KeyError}
    return req["method"], sub(req["url"]), {k: sub(v) for k, v in req["headers"].items()}, body, act


def call(method, url, headers=None, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"{}"), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}"), dict(e.headers)


def main():
    hook = ThreadingHTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=hook.serve_forever, daemon=True).start()
    srv, state = m.make_server(0, TOKEN, HOOK_KEY, f"http://127.0.0.1:{hook.server_port}/hooks/demo/rangka_whatsapp")
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    host = f"127.0.0.1:{srv.server_port}"
    base = f"http://{host}"
    auth = {"api_host": host, "service_token": TOKEN, "webhook_key": HOOK_KEY}

    def send(to, key, template="pesanan_siap", typ="order_ready", params=("Dewi", "ORD-2026-00018", "Kopi Contoh"), token=None):
        comps = [{"type": "body", "parameters": [{"type": "text", "text": p} for p in params]}]
        method, url, headers, body, act = render("send_template", {**auth, "service_token": token or TOKEN},
                                                 {"to": to, "template": template, "type": typ, "components": comps, "idempotency_key": key})
        # the manifest says https; the mock is plain http on loopback
        status, out, hdr = call(method, url.replace("https://", "http://"), headers, body)
        return status, out, hdr, act

    def sim(what, body):
        return call("POST", f"{base}/simulate/{what}", body=body)

    # The manifest itself.
    check("the connector is provided by the host", SPEC.get("experimental", {}).get("provided_by") == "host")
    check("no auth field has a default or is generated", all("default" not in f and not f.get("generate") for f in SPEC["auth"]["fields"]))
    check("the bearer and the webhook key are secret", {f["name"] for f in SPEC["auth"]["fields"] if f.get("secret")} == {"service_token", "webhook_key"})
    for a in SPEC["actions"]:
        url = a["request"]["url"]
        check(f"{a['name']}: https to the host's address, secrets only in headers",
              url.startswith("https://{auth.api_host}/") and "service_token" not in url and "service_token" not in json.dumps(a["request"]["body"]))
    check("hosts is a placeholder under .example until Rangka Cloud has an address", all(h.endswith(".example") for h in SPEC["hosts"]), SPEC["hosts"])

    # A send, built from the manifest.
    status, out, _, act = send("0812-0000-0001", "ORD-18:order_ready:1")
    check("send_template answers a message id", status == 200 and out.get("message_id", "").startswith("wam_") and out.get("status") == "accepted", (status, out))
    for name, expr in act["output"].items():
        check(f"send_template output {name} is in the answer", expr in out, out)
    first = out.get("message_id")
    status, out, _, _ = send("0812-0000-0001", "ORD-18:order_ready:1")
    check("the same key again is the first message, marked duplicate", status == 200 and out.get("message_id") == first and out.get("duplicate") is True, out)
    check("a key is sent once", len(state.messages) == 1)
    check("the action's idempotency is the key", act["idempotency"] == "input.idempotency_key")

    # Auth.
    status, out, _, _ = send("081200000001", "ORD-19:order_ready:1", token="dummy-wrong-token")
    check("a wrong bearer is 401 unauthenticated", status == 401 and out["error"]["code"] == "unauthenticated", (status, out))

    # Bad requests reach nobody.
    for field, kw in {"to": {"to": "abc"}, "type": {"typ": "promo"}, "template": {"template": "promo_akhir_tahun"}, "components": {"params": ("only",)}}.items():
        args = {"to": "081200000001", "key": "BAD:1", **kw}
        status, out, _, _ = send(args.pop("to"), args.pop("key"), **args)
        check(f"a bad {field} is 400 bad_request", status == 400 and out["error"] == {"code": "bad_request", "field": field}, (status, out))
    status, out, _, _ = send("081200000001", "has space")
    check("a bad idempotency_key is 400", status == 400 and out["error"].get("field") == "idempotency_key", (status, out))
    check("bad requests sent nothing", len(state.messages) == 1)

    # Opt-out through the manifest's set_opt_out, then a send to that customer.
    method, url, headers, body, _ = render("set_opt_out", auth, {"phone": "0812 0000 0002"})
    status, out, _ = call(method, url.replace("https://", "http://"), headers, body)
    check("set_opt_out records the customer", status == 200 and out == {"status": "recorded"}, (status, out))
    status, out, _ = call(method, url.replace("https://", "http://"), headers, body)
    check("set_opt_out again says already", out == {"status": "already"}, out)
    # PyYAML reads the key `on` as the boolean true (YAML 1.1); the kernel reads it as "on".
    retry_on = set(act["retry"].get("on") or act["retry"][True])
    status, out, _, _ = send("+6281200000002", "ORD-20:order_ready:1")
    check("an opted-out customer gets recipient_opted_out, not retried", status == 422 and out["error"]["code"] == "recipient_opted_out" and status not in retry_on, (status, out))
    check("the refusal reaches the kernel as a failed status with its code",
          got[-1] == ("message_status", {**got[-1][1], "message_id": "", "idempotency_key": "ORD-20:order_ready:1", "status": "failed", "error": "recipient_opted_out"}), got[-1:])

    # A customer writes BERHENTI: opted out, and the kernel hears opted_out.
    sim("inbound", {"from": "081200000003", "text": " berhenti "})
    check("BERHENTI posts opted_out", got[-1][0] == "opted_out" and got[-1][1]["phone"] == "+6281200000003" and got[-1][1]["source"] == "keyword", got[-1:])
    status, out, _, _ = send("081200000003", "ORD-21:order_ready:1")
    check("the next send to them is refused", status == 422, (status, out))
    sim("inbound", {"from": "081200000004", "text": "jam berapa buka?"})
    check("any other text posts message_received and opts nobody out", got[-1][0] == "message_received" and send("081200000004", "ORD-22:order_ready:1")[0] == 200, got[-1:])

    # Kill switch and pause.
    for state_name, code in (("stopped", "service_stopped"), ("paused", "service_paused")):
        sim("service", {"state": state_name})
        status, out, _, _ = send("081200000001", f"ORD-23:{state_name}")
        check(f"a {state_name} service answers {code}, not retried", status == 403 and out["error"]["code"] == code and status not in retry_on, (status, out))
    sim("service", {"state": "active"})

    # Template approval.
    sim("template", {"name": "pesanan_siap", "approved": False})
    status, out, _, _ = send("081200000001", "ORD-24:order_ready:1")
    check("an unapproved template answers template_not_approved, not retried", status == 409 and out["error"]["code"] == "template_not_approved" and status not in retry_on, (status, out))
    sim("template", {"name": "pesanan_siap", "approved": True})

    # Retried answers: rate limit and outage, then the same key goes out once.
    sim("next", {"results": ["rate_limited", "upstream_unavailable"]})
    status, out, hdr, _ = send("081200000001", "ORD-25:order_ready:1")
    check("rate_limited is 429 with Retry-After, retried", status == 429 and out["error"]["code"] == "rate_limited" and hdr.get("Retry-After") == "2" and status in retry_on, (status, out, hdr))
    status, out, _, _ = send("081200000001", "ORD-25:order_ready:1")
    check("an outage is 502, retried", status == 502 and out["error"]["code"] == "upstream_unavailable" and status in retry_on, (status, out))
    before = len(state.messages)
    status, out, _, _ = send("081200000001", "ORD-25:order_ready:1")
    check("after them the key is sent once", status == 200 and len(state.messages) == before + 1, (status, out))

    # Daily cap.
    sim("cap", {"daily": len(state.messages)})
    status, out, _, _ = send("081200000001", "ORD-26:order_ready:1")
    check("over the cap answers quota_exceeded, not retried", status == 403 and out["error"]["code"] == "quota_exceeded", (status, out))
    status, out, _, _ = send("0812-0000-0001", "ORD-18:order_ready:1")
    check("a repeat of a sent key is still answered at the cap", status == 200 and out.get("duplicate") is True, (status, out))
    sim("cap", {"daily": 500})

    # Status events: signed, with every field the trigger emits.
    n = len(got)
    for st in ("sent", "delivered", "read"):
        sim("status", {"idempotency_key": "ORD-18:order_ready:1", "status": st})
    check("three statuses reached the receiver, signed", [e[1]["status"] for e in got[n:]] == ["sent", "delivered", "read"], got[n:])
    sim("status", {"message_id": first, "status": "failed", "error": "meta:131026"})
    check("a failed status carries the error", got[-1][1]["status"] == "failed" and got[-1][1]["error"] == "meta:131026", got[-1:])
    for trig in SPEC["triggers"]:
        events = [e for t, e in got if t == trig["name"]]
        check(f"{trig['name']}: events arrived", bool(events))
        for name, expr in trig["emit"].items():
            check(f"{trig['name']}: emit {name} is in every event", all(expr in e for e in events), events[:1])
    check("a status carries the document's key and no phone", got[-1][1]["idempotency_key"] == "ORD-18:order_ready:1" and "to" not in got[-1][1])

    # Redelivery: the same bytes again; with another key the receiver refuses it.
    n, last = len(got), got[-1]
    status, out, _ = call("POST", f"{base}/simulate/redeliver", body={})
    check("a redelivered event is the same event", out.get("hook_status") == 200 and got[-1] == last and len(got) == n + 1, out)
    status, out, _ = call("POST", f"{base}/simulate/redeliver?key=dummy-other-key", body={})
    check("an event signed with another key is refused by the receiver", out.get("hook_status") == 401 and refused == ["message_status"] and len(got) == n + 1, (out, refused))

    # /calls never shows the bearer.
    status, out, _ = call("GET", f"{base}/calls")
    check("/calls redacts Authorization", TOKEN not in json.dumps(out) and any(c["authorization"] == "REDACTED" for c in out["calls"]))

    # Dummy keys only.
    for bad in (("real-looking-token", HOOK_KEY), (TOKEN, "s3cr3tkey")):
        try:
            m.make_server(0, *bad, None)
            check("a key that is not a dummy is refused", False, bad)
        except ValueError:
            check("a key that is not a dummy is refused", True)
    # the vectors of rangka-cloud TestPhoneRuleVectors and rangka-apps PHONE_VECTORS
    for raw, want in (("0812-0000-0001", "+6281200000001"), ("6281200000001", "+6281200000001"), ("+62 812 0000 0001", "+6281200000001"),
                      ("812-3456-7890", "+6281234567890"), ("+62 0812 3456 7890", "+6281234567890"), ("620812 3456 7890", "+6281234567890"),
                      ("0062 812 3456 7890", "+6281234567890"), ("+81 3 1234 5678", "+81312345678"), ("0081 3 1234 5678", "+81312345678"),
                      ("abc", None), ("0812", None)):
        check(f"phone {raw!r} normalises to {want!r}", m.normalize_phone(raw) == want, m.normalize_phone(raw))

    srv.shutdown()
    hook.shutdown()
    print(f"\n{len(fails)} failed" if fails else "\nall checks passed")
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
