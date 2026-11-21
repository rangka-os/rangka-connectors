#!/usr/bin/env python3
"""A local stand-in for Rangka Cloud's WhatsApp relay (the host side of connector `rangka_whatsapp`), for demos and
tests without Rangka Cloud and without a Meta account. Nothing is sent anywhere: a "sent" message is a row in memory.

    RANGKA_WHATSAPP_MOCK_TOKEN=dummy-wa-token-demo RANGKA_WHATSAPP_MOCK_WEBHOOK_KEY=dummy-wa-hook-demo \
        python3 rangka_whatsapp/mock/rangka_whatsapp_mock.py --port 8904 \
        --hook-url http://127.0.0.1:8080/hooks/demo/rangka_whatsapp

Dummy keys only: the token and the webhook key must say they are a dummy (a part "dummy" or "test" between - _ or .).
Keys come from the environment, never argv (`ps` shows argv). It listens on 127.0.0.1 only.

Stdlib only. The request, answer and event shapes follow connector.rk and README.md:

- POST /svc/whatsapp/v1/messages (Bearer token) {to, template, type, components, idempotency_key}: 200 {message_id,
  status}. The same idempotency_key again answers the first message with duplicate: true and sends nothing. Refusals,
  in the relay's order: service_stopped (403), service_paused (403), template_not_approved (409),
  recipient_opted_out (422), quota_exceeded (403), rate_limited (429 with Retry-After).
- POST /svc/whatsapp/v1/opt-outs (Bearer token) {phone}: 200 {status: recorded | already}.
- POST /simulate/status {message_id | idempotency_key, status, error}: the message's status changes (sent, delivered,
  read, failed) and the mock posts the message_status event to --hook-url/message_status, signed.
- POST /simulate/inbound {from, text[, lines, notes, order_signal]}: a customer writes. BERHENTI or STOP opts them out
  and posts opted_out; anything else posts message_received. The host reads a text as an order with its own parser
  (Rangka Cloud internal/whatsapp/orderparse); this mock has none, so a test passes the lines it wants the kernel to
  hear (none by default).
- POST /simulate/redeliver[?key=<other>]: posts the last event again (the host retries an event that got no 2xx),
  optionally signed with another key, to test the receiver's dedupe and its signature check.
- POST /simulate/service {state: active | stopped | paused}, POST /simulate/template {name, approved: true | false},
  POST /simulate/next {results: [rate_limited, upstream_unavailable]}, POST /simulate/cap {daily: n}.
- GET /calls: every request it received (Authorization redacted), for tests. GET /messages. GET /healthz.

A refusal the kernel does not retry is also posted as a message_status event with status failed and the code in
`error`, as the host does, so a flow can show on the document why nothing was sent.
"""
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# name -> (message type, number of parameters); the same library as Rangka Cloud's (README.md "Templates").
TEMPLATES = {
    "struk_pembayaran": ("receipt", 6),
    "tagihan_terbit": ("reminder", 6),
    "pengingat_jatuh_tempo": ("reminder", 6),
    "terima_kasih_lunas": ("reminder", 2),
    "pesanan_siap": ("order_ready", 3),
    "pengingat_janji": ("appointment", 4),
    "pesanan_diterima": ("order_reply", 5),
}
TYPES = {"receipt", "reminder", "order_ready", "appointment", "order_reply"}
STOP_WORDS = {"BERHENTI", "STOP"}
KEY_RE = re.compile(r"^[A-Za-z0-9._:/-]{1,200}$")
SIGNATURE_HEADER = "X-Rangka-Signature"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_dummy(value: str) -> bool:
    return any(p in ("dummy", "test") for p in re.split(r"[-_.]", value.lower()))


def normalize_phone(s) -> str | None:
    """Rangka Cloud's rule: '0812-0000-0001', '812 0000 0001', '62812…', '+62 0812…' -> '+6281200000001'; with a plus
    the country code is as written; None when it is not a phone."""
    s = str(s or "").strip()
    plus = s.startswith("+")
    if not re.fullmatch(r"\+?[0-9 .()\-]+", s):
        return None
    d = re.sub(r"[^0-9]", "", s)
    if not plus:
        if d.startswith("8"):
            d = "62" + d
        elif d.startswith("00"):
            d = d[2:]
        elif d.startswith("0"):
            d = "62" + d[1:]
    if d.startswith("620"):
        d = "62" + d[3:]
    if not 8 <= len(d) <= 15 or d.startswith("0"):
        return None
    return "+" + d


def sign(key: str, body: bytes) -> str:
    return "sha256=" + hmac.new(key.encode(), body, hashlib.sha256).hexdigest()


class State:
    def __init__(self, token, webhook_key, hook_url):
        self.token, self.webhook_key, self.hook_url = token, webhook_key, (hook_url or "").rstrip("/")
        self.service = "active"
        self.approved = set(TEMPLATES)
        self.opt_outs = set()
        self.messages = {}      # idempotency_key -> message
        self.next = []          # results of the next sends
        self.daily_cap = 500
        self.calls = []
        self.events = []        # (trigger, body bytes, http status)
        self.lock = threading.Lock()


def deliver(state: State, trigger: str, event: dict, key: str | None = None, body: bytes | None = None) -> int:
    """POST one event to <hook-url>/<trigger>, signed over the raw body; the HTTP status, 0 when unreachable."""
    body = body if body is not None else json.dumps(event, separators=(",", ":")).encode()
    status = 0
    if state.hook_url:
        req = urllib.request.Request(f"{state.hook_url}/{trigger}", data=body, method="POST",
                                     headers={"Content-Type": "application/json", SIGNATURE_HEADER: sign(key or state.webhook_key, body)})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                status = r.status
        except urllib.error.HTTPError as e:
            status = e.code
        except OSError:
            status = 0
    with state.lock:
        state.events.append((trigger, body, status))
    return status


def body_params(components):
    """[{type: body, parameters: [{type: text, text}]}] -> the texts, or None when it is anything else."""
    if components in (None, "", []):
        return []
    if isinstance(components, str):
        try:
            components = json.loads(components)
        except ValueError:
            return None
    if not isinstance(components, list) or len(components) != 1 or not isinstance(components[0], dict):
        return None
    c = components[0]
    if str(c.get("type", "")).lower() != "body" or not isinstance(c.get("parameters", []), list):
        return None
    out = []
    for p in c.get("parameters", []):
        if not isinstance(p, dict) or p.get("type") != "text":
            return None
        t = re.sub(r"\s+", " ", str(p.get("text", ""))).strip()
        if not t or len(t) > 512:
            return None
        out.append(t)
    return out


def handler(state: State):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, status, obj, headers=None):
            raw = json.dumps(obj).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            for k, v in (headers or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(raw)

        def _refuse(self, status, code, field=None, retry_after=None, notify=None):
            err = {"code": code}
            if field:
                err["field"] = field
            # The event goes first, so a caller that reads the answer finds the event already delivered.
            if notify:
                deliver(state, "message_status", {"message_id": "", "idempotency_key": notify["idempotency_key"], "type": notify["type"],
                                                  "status": "failed", "at": now_iso(), "error": code})
            self._json(status, {"error": err}, {"Retry-After": str(retry_after)} if retry_after else None)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n else b""
            try:
                return json.loads(raw or b"{}")
            except ValueError:
                return None

        def _authed(self):
            got = self.headers.get("Authorization", "")
            return hmac.compare_digest(got, "Bearer " + state.token)

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path == "/healthz":
                return self._json(200, {"ok": True})
            if path == "/calls":
                with state.lock:
                    return self._json(200, {"calls": list(state.calls)})
            if path == "/messages":
                with state.lock:
                    return self._json(200, {"messages": list(state.messages.values()), "opt_outs": len(state.opt_outs)})
            self._json(404, {"error": {"code": "not_found"}})

        def do_POST(self):
            url = urllib.parse.urlparse(self.path)
            path = url.path
            body = self._body()
            with state.lock:
                state.calls.append({"path": path, "authorization": "REDACTED" if self.headers.get("Authorization") else "", "body": body})
            if path.startswith("/svc/"):
                if not self._authed():
                    return self._refuse(401, "unauthenticated")
                if not isinstance(body, dict):
                    return self._refuse(400, "bad_request", "body")
                if path == "/svc/whatsapp/v1/messages":
                    return self.send_message(body)
                if path == "/svc/whatsapp/v1/opt-outs":
                    return self.opt_out(body)
            if path.startswith("/simulate/") and isinstance(body, dict):
                return self.simulate(path[len("/simulate/"):], body, urllib.parse.parse_qs(url.query))
            self._json(404, {"error": {"code": "not_found"}})

        def send_message(self, b):
            to = normalize_phone(b.get("to"))
            if not to:
                return self._refuse(400, "bad_request", "to")
            typ, tpl, key = b.get("type"), b.get("template"), b.get("idempotency_key")
            if typ not in TYPES:
                return self._refuse(400, "bad_request", "type")
            if not isinstance(key, str) or not KEY_RE.match(key):
                return self._refuse(400, "bad_request", "idempotency_key")
            if tpl not in TEMPLATES:
                return self._refuse(400, "bad_request", "template")
            params = body_params(b.get("components"))
            if params is None or TEMPLATES[tpl][0] != typ or len(params) != TEMPLATES[tpl][1]:
                return self._refuse(400, "bad_request", "components" if TEMPLATES[tpl][0] == typ else "type")
            note = {"idempotency_key": key, "type": typ}
            with state.lock:
                service, approved, opted = state.service, tpl in state.approved, to in state.opt_outs
                held = state.messages.get(key)
                count = len(state.messages)
                result = "ok"
                if service == "active" and approved and not opted and not held and count < state.daily_cap and state.next:
                    result = state.next.pop(0)
            if service == "stopped":
                return self._refuse(403, "service_stopped", notify=note)
            if service == "paused":
                return self._refuse(403, "service_paused", notify=note)
            if not approved:
                return self._refuse(409, "template_not_approved", notify=note)
            if opted:
                return self._refuse(422, "recipient_opted_out", notify=note)
            if held:
                return self._json(200, {"message_id": held["message_id"], "status": held["status"], "duplicate": True})
            if count >= state.daily_cap:
                return self._refuse(403, "quota_exceeded", notify=note)
            if result == "rate_limited":
                return self._refuse(429, "rate_limited", retry_after=2)
            if result == "upstream_unavailable":
                return self._refuse(502, "upstream_unavailable")
            msg = {"message_id": "wam_" + uuid.uuid4().hex[:20], "idempotency_key": key, "type": typ, "template": tpl,
                   "to": to, "params": params, "status": "accepted", "at": now_iso()}
            with state.lock:
                first = state.messages.setdefault(key, msg)
            if first is not msg:
                return self._json(200, {"message_id": first["message_id"], "status": first["status"], "duplicate": True})
            self._json(200, {"message_id": msg["message_id"], "status": "accepted"})

        def opt_out(self, b):
            phone = normalize_phone(b.get("phone"))
            if not phone:
                return self._refuse(400, "bad_request", "phone")
            with state.lock:
                new = phone not in state.opt_outs
                state.opt_outs.add(phone)
            self._json(200, {"status": "recorded" if new else "already"})

        def simulate(self, what, b, query):
            if what == "status":
                with state.lock:
                    msg = state.messages.get(b.get("idempotency_key")) or next(
                        (m for m in state.messages.values() if m["message_id"] == b.get("message_id")), None)
                    if msg and b.get("status") in ("sent", "delivered", "read", "failed"):
                        msg["status"] = b["status"]
                if not msg or b.get("status") not in ("sent", "delivered", "read", "failed"):
                    return self._json(404, {"error": {"code": "not_found"}})
                code = deliver(state, "message_status", {"message_id": msg["message_id"], "idempotency_key": msg["idempotency_key"],
                                                         "type": msg["type"], "status": b["status"], "at": now_iso(), "error": str(b.get("error") or "")})
                return self._json(200, {"hook_status": code})
            if what == "inbound":
                phone = normalize_phone(b.get("from"))
                if not phone:
                    return self._json(400, {"error": {"code": "bad_request", "field": "from"}})
                text = str(b.get("text") or "")
                if text.strip().upper() in STOP_WORDS:
                    with state.lock:
                        state.opt_outs.add(phone)
                    code = deliver(state, "opted_out", {"phone": phone, "at": now_iso(), "source": "keyword"})
                else:
                    code = deliver(state, "message_received", {"message_id": "wamid.IN" + uuid.uuid4().hex[:12], "from": phone, "text": text,
                                                               "kind": "text", "profile_name": str(b.get("profile_name") or "Pelanggan Contoh"),
                                                               "at": now_iso(), "lines": b.get("lines") or [], "notes": b.get("notes") or [],
                                                               "order_signal": bool(b.get("order_signal"))})
                return self._json(200, {"hook_status": code})
            if what == "redeliver":
                with state.lock:
                    last = state.events[-1] if state.events else None
                if not last:
                    return self._json(404, {"error": {"code": "not_found"}})
                code = deliver(state, last[0], {}, key=(query.get("key") or [None])[0], body=last[1])
                return self._json(200, {"hook_status": code})
            if what == "service" and b.get("state") in ("active", "stopped", "paused"):
                with state.lock:
                    state.service = b["state"]
                return self._json(200, {"state": b["state"]})
            if what == "template" and b.get("name") in TEMPLATES:
                with state.lock:
                    (state.approved.add if b.get("approved") else state.approved.discard)(b["name"])
                return self._json(200, {"name": b["name"], "approved": bool(b.get("approved"))})
            if what == "next" and isinstance(b.get("results"), list):
                with state.lock:
                    state.next.extend(str(r) for r in b["results"])
                return self._json(200, {"queued": len(b["results"])})
            if what == "cap" and isinstance(b.get("daily"), int):
                with state.lock:
                    state.daily_cap = b["daily"]
                return self._json(200, {"daily": b["daily"]})
            self._json(400, {"error": {"code": "bad_request"}})

    return H


def make_server(port: int, token: str, webhook_key: str, hook_url: str | None):
    if not is_dummy(token) or not is_dummy(webhook_key):
        raise ValueError("the mock takes dummy keys only: the token and the webhook key must have a part 'dummy' or 'test'")
    state = State(token, webhook_key, hook_url)
    return ThreadingHTTPServer(("127.0.0.1", port), handler(state)), state


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--port", type=int, default=8904)
    ap.add_argument("--hook-url", default="", help="where events go: <kernel>/hooks/<org>/rangka_whatsapp (the trigger name is appended)")
    a = ap.parse_args()
    token, key = os.environ.get("RANGKA_WHATSAPP_MOCK_TOKEN", ""), os.environ.get("RANGKA_WHATSAPP_MOCK_WEBHOOK_KEY", "")
    try:
        srv, _ = make_server(a.port, token, key, a.hook_url)
    except ValueError as e:
        sys.exit(f"rangka_whatsapp_mock: {e}")
    print(f"rangka_whatsapp mock on http://127.0.0.1:{a.port} (events to {a.hook_url or 'nowhere'})", file=sys.stderr)
    srv.serve_forever()


if __name__ == "__main__":
    main()
