#!/usr/bin/env python3
"""A local stand-in for Xendit in test mode (fixed virtual accounts and dynamic QRIS), for demos and tests without a
Xendit account.

    XENDIT_MOCK_SECRET_KEY=xnd_development_dummy_demo XENDIT_MOCK_CALLBACK_TOKEN=dummy-cb-demo \
        python3 xendit/mock/xendit_mock.py --port 8902 \
        --va-callback-url http://127.0.0.1:8790/api/xendit/va_paid --qr-callback-url http://127.0.0.1:8790/api/xendit/qr_paid

Dummy keys only: the key must start with xnd_development_ and say it is a dummy (a part "dummy" or "test" between
- _ or .), and the callback token too. Keys come from the environment, never argv (`ps` shows argv). It listens on
127.0.0.1 only.

Stdlib only. The request and callback shapes follow Xendit's API and the connector `xendit` (connector.rk):

- POST /callback_virtual_accounts (Basic auth: secret key, empty password): a fixed VA. A second request with the same
  external_id answers the VA it already made (one VA per invoice), so a retried request makes no second VA.
- POST /qr_codes (Basic auth): a dynamic QRIS, reference_id = the invoice number.
- POST /callback_virtual_accounts/external_id=<id>/simulate_payment {amount}: the payer pays the VA (Xendit test
  mode has the same endpoint). The mock sends the VA callback (x-callback-token header) and answers with the callback's
  HTTP status. A closed VA refuses another amount (400 INVALID_AMOUNT).
- POST /qr_codes/<qr id>/payments/simulate {amount}: the payer pays the QRIS; sends the qr.payment callback.
- POST /simulate/redeliver/<payment id>[?token=<other>]: sends a callback that was already sent once more (Xendit
  retries a callback that did not get a 2xx), optionally with another token, to test the receiver's idempotency and
  its token check.
- GET  /calls: every request it received (Authorization redacted), for tests. GET /healthz.
"""
from __future__ import annotations

import argparse
import base64
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
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BANKS = {"BCA": "39108", "BNI": "8808", "BRI": "26215", "MANDIRI": "88908", "PERMATA": "8214", "BSI": "6059", "CIMB": "93490"}


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


class State:
    def __init__(self, secret, token, va_url, qr_url, owner_id="5f0000000000000000000001"):
        self.secret, self.token, self.va_url, self.qr_url, self.owner_id = secret, token, va_url, qr_url, owner_id
        self.vas = {}        # external_id -> VA
        self.qrs = {}        # qr id -> QR
        self.sent = {}       # payment id -> (kind, payload)
        self.calls = []
        self.seq = 0
        self.lock = threading.Lock()


def deliver(state: State, kind: str, payload: dict, token: str | None = None) -> int:
    """POST one callback with the x-callback-token header; the HTTP status, 0 when unreachable."""
    url = state.va_url if kind == "va" else state.qr_url
    if not url:
        return 0
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "x-callback-token": token or state.token})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except OSError:
        return 0


def handler(state: State):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, code, obj):
            raw = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def authed(self) -> bool:
            h = self.headers.get("Authorization", "")
            want = "Basic " + base64.b64encode((state.secret + ":").encode()).decode()
            return hmac.compare_digest(h, want)

        def json_body(self, raw):
            try:
                return json.loads(raw or b"{}")
            except ValueError:
                return None

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path == "/healthz":
                return self.send(200, {"ok": True})
            if path == "/calls":
                with state.lock:
                    return self.send(200, state.calls)
            self.send(404, {"error_code": "NOT_FOUND"})

        def do_POST(self):
            u = urllib.parse.urlparse(self.path)
            raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
            with state.lock:
                state.calls.append({"method": "POST", "path": u.path, "body": raw.decode(errors="replace"),
                                    "headers": {k: ("[redacted]" if k.lower() == "authorization" else v) for k, v in self.headers.items()}})
            body = self.json_body(raw)
            if body is None:
                return self.send(400, {"error_code": "API_VALIDATION_ERROR", "message": "body is not JSON"})

            if u.path == "/callback_virtual_accounts":
                if not self.authed():
                    return self.send(401, {"error_code": "INVALID_API_KEY"})
                ext, bank = str(body.get("external_id") or ""), str(body.get("bank_code") or "")
                if not ext or bank not in BANKS:
                    return self.send(400, {"error_code": "API_VALIDATION_ERROR", "message": "external_id and a known bank_code are required"})
                with state.lock:
                    va = state.vas.get(ext)
                    if va is None:
                        state.seq += 1
                        amount = int(body.get("expected_amount") or 0)
                        va = {"id": uuid.uuid4().hex[:24], "owner_id": state.owner_id, "external_id": ext, "bank_code": bank,
                              "merchant_code": BANKS[bank], "name": str(body.get("name") or ""), "account_number": BANKS[bank] + f"{state.seq:011d}",
                              "expected_amount": amount, "is_closed": bool(body.get("is_closed", amount > 0)),
                              "is_single_use": bool(body.get("is_single_use", False)), "status": "PENDING", "currency": "IDR",
                              "expiration_date": body.get("expiration_date") or (datetime.now(timezone.utc) + timedelta(days=31)).strftime("%Y-%m-%dT%H:%M:%S.000Z")}
                        state.vas[ext] = va
                return self.send(200, va)

            if u.path == "/qr_codes":
                if not self.authed():
                    return self.send(401, {"error_code": "INVALID_API_KEY"})
                ref, amount = str(body.get("reference_id") or ""), int(body.get("amount") or 0)
                if not ref or amount <= 0:
                    return self.send(400, {"error_code": "API_VALIDATION_ERROR", "message": "reference_id and amount are required"})
                qid = "qr_" + uuid.uuid4().hex[:24]
                qr = {"id": qid, "reference_id": ref, "business_id": state.owner_id, "type": "DYNAMIC", "currency": "IDR", "amount": amount,
                      "channel_code": "ID_DANA", "status": "ACTIVE", "qr_string": f"00020101021226660014ID.CO.QRIS.WWW0215ID{qid[3:19]}5204599953033605404{amount}5802ID6304MOCK",
                      "expires_at": body.get("expires_at") or (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S.000Z"), "created": now_iso()}
                with state.lock:
                    state.qrs[qid] = qr
                return self.send(201, qr)

            m = re.fullmatch(r"/callback_virtual_accounts/external_id=([^/]+)/simulate_payment", u.path)
            if m:
                ext = urllib.parse.unquote(m.group(1))
                va = state.vas.get(ext)
                if not va:
                    return self.send(404, {"error_code": "CALLBACK_VIRTUAL_ACCOUNT_NOT_FOUND_ERROR"})
                if va["status"] != "PENDING":
                    return self.send(400, {"error_code": "INACTIVE_VIRTUAL_ACCOUNT_ERROR", "message": "a single-use VA is paid once"})
                amount = int(body.get("amount") or va["expected_amount"])
                if va["is_closed"] and amount != va["expected_amount"]:
                    return self.send(400, {"error_code": "INVALID_AMOUNT", "message": "a closed VA takes exactly its expected amount"})
                pid = uuid.uuid4().hex[:24]
                payload = {"id": uuid.uuid4().hex[:24], "payment_id": pid, "callback_virtual_account_id": va["id"], "owner_id": va["owner_id"],
                           "external_id": ext, "account_number": va["account_number"], "bank_code": va["bank_code"], "amount": amount,
                           "transaction_timestamp": now_iso(), "merchant_code": va["merchant_code"], "currency": "IDR"}
                with state.lock:
                    state.sent[pid] = ("va", payload)
                    if va["is_single_use"]:
                        va["status"] = "INACTIVE"
                return self.send(200, {"payment_id": pid, "callback_status": deliver(state, "va", payload)})

            m = re.fullmatch(r"/qr_codes/([^/]+)/payments/simulate", u.path)
            if m:
                qr = state.qrs.get(m.group(1))
                if not qr:
                    return self.send(404, {"error_code": "DATA_NOT_FOUND"})
                if qr["status"] != "ACTIVE":
                    return self.send(400, {"error_code": "INACTIVE_QR_CODE", "message": "a dynamic QR is paid once"})
                amount = int(body.get("amount") or qr["amount"])
                pid = "qrpy_" + uuid.uuid4().hex[:24]
                payload = {"event": "qr.payment", "business_id": qr["business_id"], "created": now_iso(),
                           "data": {"id": pid, "qr_id": qr["id"], "reference_id": qr["reference_id"], "type": "DYNAMIC", "currency": "IDR",
                                    "amount": amount, "channel_code": qr["channel_code"], "status": "SUCCEEDED", "created": now_iso()}}
                with state.lock:
                    state.sent[pid] = ("qr", payload)
                    qr["status"] = "INACTIVE"
                return self.send(200, {"payment_id": pid, "callback_status": deliver(state, "qr", payload)})

            m = re.fullmatch(r"/simulate/redeliver/([^/]+)", u.path)
            if m:
                sent = state.sent.get(m.group(1))
                if not sent:
                    return self.send(404, {"error_code": "NOT_FOUND"})
                token = urllib.parse.parse_qs(u.query).get("token", [None])[0]
                return self.send(200, {"callback_status": deliver(state, sent[0], sent[1], token)})

            self.send(404, {"error_code": "NOT_FOUND"})
    return H


def dummy(value: str) -> bool:
    """True for a value that says it is a dummy (a part dummy or test): a real key or token must never reach the mock."""
    return bool(re.search(r"(^|[-_.])(dummy|test)([-_.]|$)", value, re.IGNORECASE))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8902)
    ap.add_argument("--va-callback-url", default="", help="where VA callbacks go (the receiver's va_paid hook)")
    ap.add_argument("--qr-callback-url", default="", help="where QRIS callbacks go (the receiver's qr_paid hook)")
    a = ap.parse_args(argv)
    secret = os.environ.get("XENDIT_MOCK_SECRET_KEY", "")
    token = os.environ.get("XENDIT_MOCK_CALLBACK_TOKEN", "")
    if not (secret.startswith("xnd_development_") and dummy(secret)):
        ap.error("set XENDIT_MOCK_SECRET_KEY to a dummy test key (xnd_development_ and a part dummy, e.g. xnd_development_dummy_demo); never a real key")
    if not dummy(token):
        ap.error("set XENDIT_MOCK_CALLBACK_TOKEN to a dummy token (a part dummy or test, e.g. dummy-cb-demo)")
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), handler(State(secret, token, a.va_callback_url, a.qr_callback_url)))
    print(f"xendit mock on http://127.0.0.1:{srv.server_address[1]} (VA callbacks to {a.va_callback_url or '-'}, QRIS to {a.qr_callback_url or '-'})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
