#!/usr/bin/env python3
"""A local stand-in for the Midtrans sandbox (Snap, Core API charge, status), for demos and tests without an account.

    MIDTRANS_MOCK_SERVER_KEY=SB-Mid-server-dummy-demo python3 midtrans/mock/midtrans_mock.py --port 8903 \
        --notify-url http://127.0.0.1:8080/hooks/<org>/midtrans/payment_notified

Dummy keys only. The mock signs any notification it is asked for (/simulate/*), without authentication: it is a
signing oracle for its key. So it listens on 127.0.0.1 only, reads the key from MIDTRANS_MOCK_SERVER_KEY (not argv,
which `ps` shows), and refuses a key that does not say it is a dummy (a part "dummy" or "test" between - _ or .).
Never give it a server key from the Midtrans dashboard, sandbox or production.

Stdlib only. What it does, in Midtrans's formats (rangka connector midtrans, research in Juragan's
reports/keputusan-riset/midtrans-connector.md):

- POST /snap/v1/transactions  Basic auth (server key as the user name, empty password); answers 201 with token and
  redirect_url. An order id that was used before is 400, as at Midtrans (one transaction per order id).
- GET  /snap/v3/redirection/<token>  a small page standing in for the Snap payment page (a pay button).
- POST /v2/charge  payment_type qris, bank_transfer (bca, bni, bri, cimb answer va_numbers; permata answers
  permata_va_number) or gopay: status_code 201, transaction_status pending, actions / qr_string / expiry_time.
- GET  /v2/<order_id>/status  the transaction as Midtrans reports it (with signature_key); 404 when unknown.
- POST /simulate/pay/<order_id>[?payment_type=qris][&status=deny|expire|cancel]  marks the payment and sends the
  notification to --notify-url, the way the sandbox simulator does; answers with the notification's HTTP status.
- POST /simulate/refund/<order_id>[?amount=N]  a settled payment refunded (all, or N: partial_refund): a notification
  of transaction_status refund or partial_refund, status_code 200. Not money in.
- POST /simulate/resend/<order_id>  the last notification of that order once more (Midtrans retries).
- POST /simulate/notify  body {"order_id", "gross_amount", "transaction_status", "payment_type"}: a notification for
  an order the mock never saw (a payment made outside Snap and the Core API).
- GET  /calls  every request it received (for tests).

A notification's signature_key is hex(SHA-512(order_id + status_code + gross_amount + server key)): sign() below is
the reference the kernel's body-field verification is checked against (rangka-os contract 0002, 19 Nov 2026).
The kernel only sends requests that carry a secret over https to the hosts the connector declares (rangka-os contract
0017 §5), so a local kernel never calls this mock: the mock is the payer's side, and its API endpoints are what a
test calls with the requests the connector describes.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import html
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

WIB = timezone(timedelta(hours=7))
ORDER_ID = re.compile(r"^[A-Za-z0-9._~-]{1,50}$")   # Midtrans: letters, digits, dash, underscore, tilde and dot
# status_code per transaction_status, as Midtrans sends it: 200 success, 201 pending, 202 denied, 407 expired
STATUS_CODE = {"settlement": "200", "capture": "200", "refund": "200", "partial_refund": "200", "cancel": "200",
               "pending": "201", "deny": "202", "expire": "407"}


def sign(order_id: str, status_code: str, gross_amount: str, server_key: str) -> str:
    """Midtrans signature_key: hex(SHA-512(order_id + status_code + gross_amount + server key)), no separators."""
    return hashlib.sha512((order_id + status_code + gross_amount + server_key).encode()).hexdigest()


def now_wib() -> str:
    return datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")


def money(amount: int) -> str:
    return f"{int(amount)}.00"


class State:
    """The mock's memory. server_key is a dummy key: whoever reaches the mock can have anything signed with it."""

    def __init__(self, server_key, notify_url, merchant_id="G000000001"):
        self.server_key, self.notify_url, self.merchant_id = server_key, notify_url, merchant_id
        self.orders = {}    # order_id -> {transaction_id, gross_amount, payment_type, status, refunded, extra, last}
        self.tokens = {}    # snap token -> order_id
        self.calls = []
        self.lock = threading.Lock()


def body_of(state: State, order_id: str, o: dict, status: str, **more) -> dict:
    """A notification (and status answer) for an order, signed."""
    code = STATUS_CODE.get(status, "200")
    gross = money(o["gross_amount"])
    d = {"transaction_time": o["time"], "transaction_status": status, "transaction_id": o["transaction_id"],
         "status_message": "midtrans payment notification", "status_code": code,
         "signature_key": sign(order_id, code, gross, state.server_key), "payment_type": o["payment_type"],
         "order_id": order_id, "merchant_id": state.merchant_id, "gross_amount": gross, "fraud_status": "accept",
         "currency": "IDR"}
    if status in ("settlement", "refund", "partial_refund"):
        d["settlement_time"] = o.get("settled") or now_wib()
    d.update(o.get("extra") or {})
    d.update(more)
    return d


def post(url: str, body: dict) -> int:
    """Send one notification as Midtrans does; returns the HTTP status (0 when unreachable)."""
    req = urllib.request.Request(url, data=json.dumps(body, separators=(",", ":")).encode(), method="POST",
                                 headers={"Content-Type": "application/json", "User-Agent": "Veritrans (mock)"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except OSError:
        return 0


def notify(state: State, order_id: str, o: dict, status: str, **more) -> int:
    body = body_of(state, order_id, o, status, **more)
    o["last"] = body
    return post(state.notify_url, body)


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

        def authorized(self) -> bool:
            want = "Basic " + base64.b64encode((state.server_key + ":").encode()).decode()
            return hmac.compare_digest(self.headers.get("Authorization", ""), want)

        def refuse(self):
            self.send(401, {"status_code": "401", "status_message": "Transaction cannot be authorized with the current client/server key.",
                            "id": str(uuid.uuid4())})

        def host(self) -> str:
            return self.headers.get("Host", "127.0.0.1")

        def do_GET(self):
            path = urllib.parse.urlparse(self.path).path
            if path == "/calls":
                with state.lock:
                    return self.send(200, state.calls)
            if path.startswith("/snap/v3/redirection/"):
                order_id = state.tokens.get(path.rsplit("/", 1)[-1])
                o = state.orders.get(order_id)
                if not o:
                    return self.send(404, {"error_messages": ["token not found"]})
                return self.send(200, f"<!doctype html><meta charset=utf-8><title>Midtrans Snap (mock)</title>"
                                      f"<h1>Bayar {html.escape(order_id)}</h1><p>Rp {o['gross_amount']:,}</p>"
                                      f"<form method=post action=/simulate/pay/{urllib.parse.quote(order_id, safe='')}><button>Simulasi bayar QRIS</button></form>",
                                 "text/html; charset=utf-8")
            m = re.match(r"^/v2/(qris|gopay)/([^/]+)/qr-code$", path)
            if m:
                for o in state.orders.values():
                    if o["transaction_id"] == m.group(2):
                        return self.send(200, o.get("qr_string", ""), "text/plain; charset=utf-8")
                return self.send(404, {"status_code": "404", "status_message": "Transaction doesn't exist.", "id": str(uuid.uuid4())})
            m = re.match(r"^/v2/([^/]+)/status$", path)
            if m:
                if not self.authorized():
                    return self.refuse()
                order_id = urllib.parse.unquote(m.group(1))
                o = state.orders.get(order_id)
                if not o or not o.get("status"):
                    return self.send(404, {"status_code": "404", "status_message": "Transaction doesn't exist.", "id": str(uuid.uuid4())})
                more = {"status_message": "Success, transaction is found"}
                if o.get("refunded"):
                    more["refund_amount"] = money(o["refunded"])
                return self.send(200, body_of(state, order_id, o, o["status"], **more))
            self.send(404, {"status_code": "404", "status_message": "The requested resource is not found"})

        def new_order(self, order_id, amount, payment_type, status, **extra):
            """One transaction per order id; None when the id was used before."""
            with state.lock:
                if order_id in state.orders:
                    return None
                o = {"transaction_id": str(uuid.uuid4()), "gross_amount": amount, "payment_type": payment_type,
                     "status": status, "refunded": 0, "time": now_wib(), "extra": extra}
                state.orders[order_id] = o
                return o

        def do_POST(self):
            u = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(u.query)
            raw = self.body()
            with state.lock:
                state.calls.append({"method": "POST", "path": u.path, "headers": {k: v for k, v in self.headers.items() if k.lower() != "authorization"},
                                    "body": raw.decode(errors="replace")})
            if u.path in ("/snap/v1/transactions", "/v2/charge"):
                if not self.authorized():
                    return self.refuse()
                try:
                    req = json.loads(raw)
                    td = req["transaction_details"]
                    order_id, amount = str(td["order_id"]), int(float(td["gross_amount"]))
                except (ValueError, KeyError, TypeError):
                    return self.send(400, {"status_code": "400", "status_message": "One or more parameters in the payload is invalid.",
                                           "error_messages": ["transaction_details.order_id is required"]})
                if not ORDER_ID.match(order_id) or amount <= 0:
                    return self.send(400, {"status_code": "400", "status_message": "One or more parameters in the payload is invalid.",
                                           "error_messages": ["transaction_details.order_id format is invalid" if amount > 0 else "transaction_details.gross_amount is not equal to the sum of item_details"]})
                if u.path == "/snap/v1/transactions":
                    # no payment method yet: the payer picks one on the page (the mock's button pays by QRIS)
                    if self.new_order(order_id, amount, "qris", "") is None:
                        return self.send(400, {"error_messages": ["transaction_details.order_id sudah digunakan"]})
                    token = str(uuid.uuid4())
                    state.tokens[token] = order_id
                    return self.send(201, {"token": token, "redirect_url": f"http://{self.host()}/snap/v3/redirection/{token}"})
                ptype = req.get("payment_type")
                if ptype not in ("qris", "bank_transfer", "gopay"):
                    return self.send(400, {"status_code": "400", "status_message": "One or more parameters in the payload is invalid.",
                                           "error_messages": ["payment_type is not supported by the mock"]})
                extra = {}
                if ptype == "bank_transfer":
                    bank = str((req.get("bank_transfer") or {}).get("bank") or "")
                    if bank not in ("bca", "bni", "bri", "cimb", "permata"):
                        return self.send(400, {"status_code": "400", "status_message": "One or more parameters in the payload is invalid.",
                                               "error_messages": ["bank_transfer.bank is invalid"]})
                    va = "8127" + f"{int(hashlib.sha256(order_id.encode()).hexdigest(), 16) % 10**11:011d}"
                    extra = {"permata_va_number": va} if bank == "permata" else {"va_numbers": [{"bank": bank, "va_number": va}]}
                if ptype == "qris":
                    extra = {"acquirer": str((req.get("qris") or {}).get("acquirer") or "gopay")}
                o = self.new_order(order_id, amount, ptype, "pending", **extra)
                if o is None:
                    return self.send(406, {"status_code": "406", "status_message": "The request could not be completed due to a conflict with the current state of the target resource, please try again",
                                           "id": str(uuid.uuid4())})
                tid, base = o["transaction_id"], f"http://{self.host()}"
                exp = req.get("custom_expiry") or {}
                minutes = int(exp.get("expiry_duration") or (1440 if ptype == "bank_transfer" else 15))
                ans = {"status_code": "201", "transaction_id": tid, "order_id": order_id, "merchant_id": state.merchant_id,
                       "gross_amount": money(amount), "currency": "IDR", "payment_type": ptype, "transaction_time": o["time"],
                       "transaction_status": "pending", "fraud_status": "accept",
                       "expiry_time": (datetime.now(WIB) + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")}
                ans.update(extra)
                if ptype == "qris":
                    o["qr_string"] = f"00020101021226MOCK{tid.replace('-', '')}5303360540{len(str(amount))}{amount}5802ID6304MOCK"
                    ans.update({"status_message": "QRIS transaction is created", "qr_string": o["qr_string"],
                                "actions": [{"name": "generate-qr-code", "method": "GET", "url": f"{base}/v2/qris/{tid}/qr-code"}]})
                elif ptype == "gopay":
                    o["qr_string"] = f"gopay-mock-{tid}"
                    ans.update({"status_message": "GO-PAY billing created",
                                "actions": [{"name": "generate-qr-code", "method": "GET", "url": f"{base}/v2/gopay/{tid}/qr-code"},
                                            {"name": "deeplink-redirect", "method": "GET", "url": f"{base}/snap/v3/redirection/gopay-{tid}"},
                                            {"name": "get-status", "method": "GET", "url": f"{base}/v2/{tid}/status"},
                                            {"name": "cancel", "method": "POST", "url": f"{base}/v2/{tid}/cancel"}]})
                else:
                    ans["status_message"] = "Success, Bank Transfer transaction is created"
                return self.send(200, ans)
            m = re.match(r"^/simulate/(pay|refund|resend)/(.+)$", u.path)
            if m:
                kind, order_id = m.group(1), urllib.parse.unquote(m.group(2))
                o = state.orders.get(order_id)
                if not o:
                    return self.send(404, {"error": "no such order"})
                if kind == "pay":
                    status = q.get("status", ["settlement"])[0]
                    if status not in STATUS_CODE or status in ("refund", "partial_refund"):
                        return self.send(400, {"error": "status is settlement, capture, pending, deny, expire or cancel"})
                    if o["status"] in ("settlement", "capture", "refund", "partial_refund"):
                        return self.send(409, {"error": "already paid: use /simulate/resend for a repeated notification"})
                    if "payment_type" in q:
                        o["payment_type"] = q["payment_type"][0]
                    o["status"] = status
                    if status == "settlement":
                        o["settled"] = now_wib()
                    return self.send(200, {"notified": notify(state, order_id, o, status), "status": status})
                if kind == "refund":
                    if o["status"] not in ("settlement", "capture", "partial_refund"):
                        return self.send(404, {"error": "not paid"})
                    if o["payment_type"] == "bank_transfer":
                        return self.send(412, {"status_code": "412", "status_message": "Merchant cannot modify the status of the transaction"})
                    left = o["gross_amount"] - o["refunded"]
                    amount = int(q.get("amount", [str(left)])[0])
                    if amount <= 0 or amount > left:
                        return self.send(400, {"error": "amount is more than what is left to refund"})
                    o["refunded"] += amount
                    o["status"] = "refund" if o["refunded"] == o["gross_amount"] else "partial_refund"
                    code = notify(state, order_id, o, o["status"], refund_amount=money(o["refunded"]),
                                  refunds=[{"refund_chargeback_id": 1, "refund_amount": money(amount), "created_at": now_wib(),
                                            "reason": "refund from the mock", "refund_key": "refund-" + uuid.uuid4().hex[:10]}])
                    return self.send(200, {"notified": code, "status": o["status"], "amount": amount})
                if not o.get("last"):
                    return self.send(404, {"error": "nothing was notified for this order"})
                return self.send(200, {"notified": post(state.notify_url, o["last"]), "status": o["last"]["transaction_status"]})
            if u.path == "/simulate/notify":
                try:
                    req = json.loads(raw or b"{}")
                    order_id, status = str(req["order_id"]), str(req.get("transaction_status", "settlement"))
                    o = {"transaction_id": str(req.get("transaction_id") or uuid.uuid4()), "gross_amount": int(req["gross_amount"]),
                         "payment_type": str(req.get("payment_type", "qris")), "time": now_wib(), "extra": {}}
                    more = {k: req[k] for k in ("fraud_status", "status_code", "refund_amount") if k in req}
                    if "status_code" in more:   # a chosen status code is signed as sent
                        more["signature_key"] = sign(order_id, str(more["status_code"]), money(o["gross_amount"]), state.server_key)
                except (ValueError, KeyError, TypeError):
                    return self.send(400, {"error": "order_id and gross_amount are needed"})
                return self.send(200, {"notified": post(state.notify_url, body_of(state, order_id, o, status, **more))})
            self.send(404, {"status_code": "404", "status_message": "The requested resource is not found"})
    return H


def dummy_key(key: str) -> bool:
    """True for a key that says it is a dummy: the mock signs whatever it is asked, so a real key must never reach it."""
    return bool(re.search(r"(^|[-_.])(dummy|test)([-_.]|$)", key, re.IGNORECASE))


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--port", type=int, default=8903)
    ap.add_argument("--notify-url", required=True, help="the kernel hook: http://<kernel>/hooks/<org>/midtrans/payment_notified")
    a = ap.parse_args(argv)
    key = os.environ.get("MIDTRANS_MOCK_SERVER_KEY") or ""
    if not dummy_key(key):
        ap.error("set MIDTRANS_MOCK_SERVER_KEY to a dummy key (a part dummy or test, e.g. SB-Mid-server-dummy-demo); never a real Midtrans key")
    srv = ThreadingHTTPServer(("127.0.0.1", a.port), handler(State(key, a.notify_url)))
    print(f"midtrans mock on http://127.0.0.1:{srv.server_address[1]} (notifications to {a.notify_url})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
