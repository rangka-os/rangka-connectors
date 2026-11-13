#!/usr/bin/env python3
"""A local stand-in for the DOKU Non-SNAP Checkout API, for demos and tests without a DOKU account.

    DOKU_MOCK_SECRET_KEY=SK-test-dummy-demo python3 doku/mock/doku_mock.py --port 8901 --client-id BRN-0001-CONTOH \
        --notify-url http://127.0.0.1:8080/hooks/<org>/doku/payment_notified

Dummy keys only. The mock signs any notification it is asked for (/simulate/*), without authentication: it is a
signing oracle for its key. So it listens on 127.0.0.1 only, reads the key from DOKU_MOCK_SECRET_KEY (not argv, which
`ps` shows), and refuses a key that does not say it is a dummy (a part "dummy" or "test" between - _ or .). Never give it a
key from DOKU Back Office, sandbox or production.

Stdlib only. What it does, in DOKU's formats (rangka connector doku, research in Juragan's
reports/keputusan-riset/doku-connector.md):

- POST /checkout/v1/payment  checks Client-Id and the signature (HMACSHA256= + base64 HMAC-SHA256 over Client-Id,
  Request-Id, Request-Timestamp, Request-Target and Digest lines), answers like DOKU Checkout (response.payment.url,
  token_id, expired_date). A bad signature is 401 {"error": {"code": "invalid_signature"}}.
- GET  /checkout-link-v2/<token>  a small page standing in for DOKU's payment page (QRIS text and a pay button).
- POST /simulate/pay/<token>[?status=FAILED]  marks the payment and sends the signed HTTP notification to
  --notify-url, the way DOKU's sandbox simulator does; answers with the notification's status code.
- POST /simulate/refund/<token>[?amount=N]  a paid payment refunded (all, or N): a notification of transaction.type
  REFUND, status SUCCESS, same original_request_id, as DOKU sends after a refund from Back Office. Not money in.
- POST /simulate/notify  body {"invoice_number", "amount", "status", "type"}: a notification for an invoice without
  Checkout (a static QRIS paid at the counter); type SALE by default, REFUND for a refund.
- GET  /calls  every request it received (for tests).

The kernel only sends signed requests over https to api-sandbox.doku.com or api.doku.com (rangka-os contract 0017
§5), so a local kernel never calls this mock: the mock is the payer's side. Its Checkout endpoint is what a test calls
with requests signed the same way, and sign_lines() below is the reference used to check the kernel's signatures.
"""
from __future__ import annotations

import argparse
import base64
import html
import os
import re
import hashlib
import hmac
import json
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def digest(body: bytes) -> str:
    return base64.b64encode(hashlib.sha256(body).digest()).decode()


def sign_lines(secret: str, client_id: str, request_id: str, timestamp: str, target: str, body: bytes | None) -> str:
    """DOKU Non-SNAP signature: HMACSHA256= + base64(HMAC-SHA256(secret, lines)); no Digest line without a body."""
    lines = [f"Client-Id:{client_id}", f"Request-Id:{request_id}", f"Request-Timestamp:{timestamp}", f"Request-Target:{target}"]
    if body:
        lines.append(f"Digest:{digest(body)}")
    mac = hmac.new(secret.encode(), "\n".join(lines).encode(), hashlib.sha256).digest()
    return "HMACSHA256=" + base64.b64encode(mac).decode()


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class State:
    """The mock's memory. secret is a dummy key: whoever reaches the mock can have anything signed with it."""

    def __init__(self, client_id, secret, notify_url):
        self.client_id, self.secret, self.notify_url = client_id, secret, notify_url
        self.payments = {}   # token -> {invoice_number, amount, request_id, status}
        self.calls = []
        self.lock = threading.Lock()


def notify(state: State, invoice: str, amount: int, status: str, original_request_id: str, channel="QRIS", type_="SALE") -> int:
    """Send one signed notification to the notify URL, as DOKU does; returns the HTTP status (0 when unreachable)."""
    body = json.dumps({
        "order": {"invoice_number": invoice, "amount": amount},
        "transaction": {"type": type_, "status": status, "date": now_utc(), "original_request_id": original_request_id},
        "service": {"id": "QRIS" if channel == "QRIS" else "VIRTUAL_ACCOUNT"}, "acquirer": {"id": "QRIS"},
        "channel": {"id": channel}, "additional_info": {"origin": {"product": "CHECKOUT", "source": "doku-mock"}},
    }, separators=(",", ":")).encode()
    req_id, ts = str(uuid.uuid4()), now_utc()
    target = urllib.parse.urlparse(state.notify_url).path
    req = urllib.request.Request(state.notify_url, data=body, method="POST", headers={
        "Content-Type": "application/json", "Client-Id": state.client_id, "Request-Id": req_id, "Request-Timestamp": ts,
        "Signature": sign_lines(state.secret, state.client_id, req_id, ts, target, body)})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except OSError:
        return 0


def handler(state: State):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, code, obj, ctype="application/json"):
            raw = obj.encode() if isinstance(obj, str) else json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def body(self) -> bytes:
            n = int(self.headers.get("Content-Length") or 0)
            return self.rfile.read(n) if n else b""

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path == "/calls":
                with state.lock:
                    return self.send(200, state.calls)
            if path.startswith("/checkout-link-v2/"):
                tok = path.rsplit("/", 1)[-1]
                p = state.payments.get(tok)
                if not p:
                    return self.send(404, {"error": {"code": "not_found"}})
                return self.send(200, f"<!doctype html><meta charset=utf-8><title>DOKU (mock)</title>"
                                      f"<h1>Bayar {html.escape(str(p['invoice_number']))}</h1><p>QRIS Rp {p['amount']:,}</p>"
                                      f"<form method=post action=/simulate/pay/{urllib.parse.quote(tok)}><button>Simulasi bayar</button></form>",
                                 "text/html; charset=utf-8")
            self.send(404, {"error": {"code": "not_found"}})

        def do_POST(self):
            u = urllib.parse.urlparse(self.path)
            raw = self.body()
            with state.lock:
                state.calls.append({"method": "POST", "path": u.path, "headers": {k: v for k, v in self.headers.items()},
                                    "body": raw.decode(errors="replace")})
            if u.path == "/checkout/v1/payment":
                h = self.headers
                want = sign_lines(state.secret, state.client_id, h.get("Request-Id", ""), h.get("Request-Timestamp", ""), u.path, raw)
                if h.get("Client-Id") != state.client_id or not hmac.compare_digest(h.get("Signature", ""), want):
                    return self.send(401, {"error": {"code": "invalid_signature", "message": "Invalid Header Signature"}})
                try:
                    req = json.loads(raw)
                    order, pay = req["order"], req.get("payment", {})
                    amount = int(float(order["amount"]))
                    invoice = str(order["invoice_number"])
                except (ValueError, KeyError, TypeError):
                    return self.send(400, {"error": {"code": "invalid_request"}})
                tok = "tok-" + uuid.uuid4().hex[:12]
                due = int(pay.get("payment_due_date") or 60)
                with state.lock:
                    state.payments[tok] = {"invoice_number": invoice, "amount": amount, "request_id": h.get("Request-Id"), "status": "PENDING"}
                host = self.headers.get("Host", "127.0.0.1")
                expires = (datetime.now(timezone(timedelta(hours=7))) + timedelta(minutes=due)).strftime("%Y%m%d%H%M%S")
                return self.send(200, {"message": ["SUCCESS"], "response": {
                    "order": {"amount": str(amount), "invoice_number": invoice, "currency": "IDR", "session_id": "sess-" + tok},
                    "payment": {"payment_method_types": pay.get("payment_method_types") or ["QRIS"], "payment_due_date": due,
                                "token_id": tok, "url": f"http://{host}/checkout-link-v2/{tok}", "expired_date": expires},
                    "headers": {"request_id": h.get("Request-Id")}}})
            if u.path.startswith("/simulate/pay/"):
                tok = u.path.rsplit("/", 1)[-1]
                p = state.payments.get(tok)
                if not p:
                    return self.send(404, {"error": {"code": "not_found"}})
                status = urllib.parse.parse_qs(u.query).get("status", ["SUCCESS"])[0]
                p["status"] = status
                code = notify(state, p["invoice_number"], p["amount"], status, p["request_id"])
                return self.send(200, {"notified": code, "status": status})
            if u.path.startswith("/simulate/refund/"):
                tok = u.path.rsplit("/", 1)[-1]
                p = state.payments.get(tok)
                if not p or p["status"] != "SUCCESS":
                    return self.send(404, {"error": {"code": "not_paid"}})
                amount = int(urllib.parse.parse_qs(u.query).get("amount", [str(p["amount"])])[0])
                code = notify(state, p["invoice_number"], amount, "SUCCESS", p["request_id"], type_="REFUND")
                return self.send(200, {"notified": code, "type": "REFUND", "amount": amount})
            if u.path == "/simulate/notify":
                try:
                    req = json.loads(raw or b"{}")
                    code = notify(state, str(req["invoice_number"]), int(req["amount"]), req.get("status", "SUCCESS"),
                                  req.get("original_request_id") or str(uuid.uuid4()), req.get("channel", "QRIS"),
                                  req.get("type", "SALE"))
                except (ValueError, KeyError, TypeError):
                    return self.send(400, {"error": {"code": "invalid_request"}})
                return self.send(200, {"notified": code})
            self.send(404, {"error": {"code": "not_found"}})
    return H


def dummy_key(secret: str) -> bool:
    """True for a key that says it is a dummy: the mock signs whatever it is asked, so a real key must never reach it."""
    return bool(re.search(r"(^|[-_.])(dummy|test)([-_.]|$)", secret, re.IGNORECASE))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8901)
    ap.add_argument("--client-id", required=True)
    ap.add_argument("--secret-key", help="deprecated (visible in ps): set DOKU_MOCK_SECRET_KEY instead")
    ap.add_argument("--notify-url", required=True, help="the kernel hook: http://<kernel>/hooks/<org>/doku/payment_notified")
    a = ap.parse_args(argv)
    secret = os.environ.get("DOKU_MOCK_SECRET_KEY") or a.secret_key or ""
    if not dummy_key(secret):
        ap.error("set DOKU_MOCK_SECRET_KEY to a dummy key (a part dummy or test, e.g. SK-test-dummy-demo); never a real DOKU key")
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), handler(State(a.client_id, secret, a.notify_url)))
    print(f"doku mock on http://127.0.0.1:{srv.server_address[1]} (notifications to {a.notify_url})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
