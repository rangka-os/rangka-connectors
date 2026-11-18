#!/usr/bin/env python3
"""Check midtrans/mock/midtrans_mock.py: its signature equals the vector rangka-os checks the kernel against, the
fixtures carry the signed fields, Snap and the Core API refuse a wrong key and answer in Midtrans's shape
with every field the connector's outputs read, and a simulated payment, refund or expiry reaches the notify URL signed
so that the receiver recomputes signature_key from the body it got. Stdlib only, no network beyond 127.0.0.1.

    python3 tests/check_midtrans_mock.py
"""
import base64
import glob
import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "midtrans", "mock"))
import midtrans_mock as m  # noqa: E402

KEY = "SB-Mid-server-dummy-test-0001"
fails = []


def check(name, cond, detail=""):
    print(("ok    " if cond else "FAIL  ") + name + ("" if cond else f"  {detail}"))
    if not cond:
        fails.append(name)


# 1. the vector of rangka-os kernel/internal/server/connectors_bodysig_test.go (computed outside Go and outside this mock)
VECTOR = open(os.path.join(ROOT, "midtrans", "fixtures", "signature_vector.txt")).read().split()[0]
check("signature = kernel vector", m.sign("INV-2026-00001", "200", "750000.00", KEY) == VECTOR)
check("the amount is signed as text: 750000 and 750000.00 differ", m.sign("INV-2026-00001", "200", "750000", KEY) != VECTOR)

# 2. the notification fixtures leave the hash out (a secret scanner takes 128 hex digits for a key): signed with the
# dummy key they are what the mock would send, so the signed fields must be there as texts
for f in sorted(glob.glob(os.path.join(ROOT, "midtrans", "fixtures", "payment_notified*.json"))):
    d = json.load(open(f))
    check(f"fixture {os.path.basename(f)} has the three signed fields as texts",
          all(isinstance(d.get(k), str) and d[k] for k in ("order_id", "status_code", "gross_amount")) and "signature_key" in d)

# 3. a receiver standing in for the kernel hook: recomputes the signature from the body, as the kernel does
got = []


class Hook(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        d = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)))
        ok = d.get("signature_key") == m.sign(d.get("order_id", ""), d.get("status_code", ""), d.get("gross_amount", ""), KEY)
        got.append((ok, d))
        self.send_response(200 if ok else 401)
        self.end_headers()


hook = ThreadingHTTPServer(("127.0.0.1", 0), Hook)
threading.Thread(target=hook.serve_forever, daemon=True).start()
notify_url = f"http://127.0.0.1:{hook.server_address[1]}/hooks/tahu_contoh/midtrans/payment_notified"
mock = ThreadingHTTPServer(("127.0.0.1", 0), m.handler(m.State(KEY, notify_url)))
threading.Thread(target=mock.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{mock.server_address[1]}"
BASIC = {"Authorization": "Basic " + base64.b64encode((KEY + ":").encode()).decode()}


def call(method, path, body=None, headers=None):
    data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    req = urllib.request.Request(base + path, data=data, method=method, headers=dict({"Content-Type": "application/json"}, **(headers or {})))
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if r.headers.get("Content-Type", "").startswith("application/json") else raw.decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def order(order_id, amount):
    return {"transaction_details": {"order_id": order_id, "gross_amount": amount}}


# Snap: Basic auth with the server key, one transaction per order id
st, out = call("POST", "/snap/v1/transactions", order("INV-2026-00007", 750000))
check("Snap without the key: 401", st == 401 and out["status_code"] == "401", (st, out))
st, out = call("POST", "/snap/v1/transactions", order("INV-2026-00007", 750000),
               {"Authorization": "Basic " + base64.b64encode(b"SB-Mid-server-other-key:").decode()})
check("Snap with another key: 401", st == 401, (st, out))
st, snap = call("POST", "/snap/v1/transactions", dict(order("INV-2026-00007", 750000), enabled_payments=["other_qris"]), BASIC)
check("Snap answers 201 with token and redirect_url", st == 201 and snap.get("redirect_url", "").endswith(snap.get("token", "?")), (st, snap))
st, out = call("POST", "/snap/v1/transactions", order("INV-2026-00007", 750000), BASIC)
check("the same order id again: refused (400)", st == 400 and "error_messages" in out, (st, out))
st, out = call("POST", "/snap/v1/transactions", order("INV-2026-00007~2", 750000), BASIC)
check("the next attempt (<number>~2) is a new transaction", st == 201, (st, out))
st, out = call("POST", "/snap/v1/transactions", order("INV/TC/26/0007", 750000), BASIC)
check("an order id with a slash is refused (400)", st == 400, (st, out))
st, out = call("GET", "/v2/INV-2026-00007/status", headers=BASIC)
check("status before the payer chose a method: 404, status_code 404", st == 404 and out["status_code"] == "404", (st, out))

# a simulated payment of the Snap page: signed notification, settlement, status_code 200
st, out = call("POST", "/simulate/pay/INV-2026-00007", b"")
n = got[-1][1] if got else {}
check("simulated payment notifies, receiver verifies signature_key", st == 200 and out["notified"] == 200 and got and got[-1][0], (st, out, got[-1:]))
check("notification: order id, amount as text with two decimals, settlement, status_code 200, fraud accept, qris",
      n.get("order_id") == "INV-2026-00007" and n.get("gross_amount") == "750000.00" and n.get("transaction_status") == "settlement"
      and n.get("status_code") == "200" and n.get("fraud_status") == "accept" and n.get("payment_type") == "qris"
      and n.get("transaction_id") and n.get("transaction_time") and n.get("settlement_time"), n)
count = len(got)
st, out = call("POST", "/simulate/resend/INV-2026-00007", b"")
check("resend: the same notification again, byte for byte the same fields", st == 200 and len(got) == count + 1 and got[-1][1] == n, (st, out))
st, out = call("POST", "/simulate/pay/INV-2026-00007", b"")
check("paying a paid order again is refused (409)", st == 409, (st, out))
st, out = call("GET", "/v2/INV-2026-00007/status", headers=BASIC)
check("status of the paid order: settlement, 200, a valid signature_key",
      st == 200 and out["transaction_status"] == "settlement" and out["status_code"] == "200"
      and out["signature_key"] == m.sign("INV-2026-00007", "200", "750000.00", KEY), (st, out))
st, out = call("GET", "/v2/INV-2026-00007/status")
check("status without the key: 401", st == 401, (st, out))
st, out = call("GET", "/v2/INV-TIDAK-ADA/status", headers=BASIC)
check("status of an unknown order: 404 in Midtrans's shape", st == 404 and out["status_message"] == "Transaction doesn't exist.", (st, out))

# refunds: partial, then the rest; never more than was paid
st, out = call("POST", "/simulate/refund/INV-2026-00007?amount=250000", b"")
r = got[-1][1]
check("partial refund: signed, partial_refund, status_code 200, refund_amount, gross_amount unchanged",
      st == 200 and got[-1][0] and r["transaction_status"] == "partial_refund" and r["status_code"] == "200"
      and r["refund_amount"] == "250000.00" and r["gross_amount"] == "750000.00" and r["refunds"][0]["refund_amount"] == "250000.00", (st, out, r))
st, out = call("POST", "/simulate/refund/INV-2026-00007?amount=600000", b"")
check("a refund of more than what is left is refused", st == 400, (st, out))
st, out = call("POST", "/simulate/refund/INV-2026-00007", b"")
r = got[-1][1]
check("the rest refunded: refund, refund_amount the whole payment", st == 200 and r["transaction_status"] == "refund" and r["refund_amount"] == "750000.00", (st, out, r))
st, out = call("POST", "/simulate/refund/INV-2026-00007~2", b"")
check("refund of an unpaid order: 404", st == 404, (st, out))

# Core API charges: the fields the connector's outputs read
st, out = call("POST", "/v2/charge", dict(order("INV-2026-00008", 400000), payment_type="qris", qris={"acquirer": "gopay"}))
check("charge without the key: 401", st == 401, (st, out))
st, qr = call("POST", "/v2/charge", dict(order("INV-2026-00008", 400000), payment_type="qris", qris={"acquirer": "gopay"},
                                         custom_expiry={"expiry_duration": 15, "unit": "minute"}), BASIC)
check("QRIS charge: status_code 201, pending, generate-qr-code action, qr_string, expiry_time",
      st == 200 and qr["status_code"] == "201" and qr["transaction_status"] == "pending" and qr["order_id"] == "INV-2026-00008"
      and qr["gross_amount"] == "400000.00" and qr["actions"][0]["name"] == "generate-qr-code" and qr.get("qr_string") and qr.get("expiry_time"), (st, qr))
st, out = call("GET", qr["actions"][0]["url"].replace(base, ""))
check("the QR action answers the QRIS text", st == 200 and out == qr["qr_string"], (st, out))
st, out = call("POST", "/v2/charge", dict(order("INV-2026-00008", 400000), payment_type="qris"), BASIC)
check("a second charge with the same order id: 406", st == 406 and out["status_code"] == "406", (st, out))
st, out = call("GET", "/v2/INV-2026-00008/status", headers=BASIC)
check("status of a pending charge: pending, 201", st == 200 and out["transaction_status"] == "pending" and out["status_code"] == "201", (st, out))
st, out = call("POST", "/simulate/pay/INV-2026-00008?status=expire", b"")
e = got[-1][1]
check("an expired QR: signed notification, expire, status_code 407", st == 200 and got[-1][0] and e["transaction_status"] == "expire" and e["status_code"] == "407", (st, out, e))
st, va = call("POST", "/v2/charge", dict(order("INV-2026-00009", 120000), payment_type="bank_transfer", bank_transfer={"bank": "bca"}), BASIC)
check("BCA VA charge: va_numbers with the bank and a number", st == 200 and va["va_numbers"][0]["bank"] == "bca" and va["va_numbers"][0]["va_number"].isdigit()
      and va["transaction_status"] == "pending", (st, va))
st, pva = call("POST", "/v2/charge", dict(order("INV-2026-00010", 120000), payment_type="bank_transfer", bank_transfer={"bank": "permata"}), BASIC)
check("Permata VA charge: permata_va_number, no va_numbers", st == 200 and pva.get("permata_va_number", "").isdigit() and "va_numbers" not in pva, (st, pva))
st, out = call("POST", "/simulate/pay/INV-2026-00009", b"")
v = got[-1][1]
check("a paid VA: settlement with va_numbers, payment_type bank_transfer", st == 200 and got[-1][0] and v["payment_type"] == "bank_transfer"
      and v["va_numbers"] == va["va_numbers"] and v["transaction_status"] == "settlement", (st, out, v))
st, out = call("POST", "/simulate/refund/INV-2026-00009", b"")
check("a virtual account payment cannot be refunded (412)", st == 412, (st, out))
st, gp = call("POST", "/v2/charge", dict(order("INV-2026-00011", 85000), payment_type="gopay", gopay={"enable_callback": True, "callback_url": "https://contoh.test/kembali"}), BASIC)
check("GoPay charge: generate-qr-code and deeplink-redirect actions", st == 200 and [a["name"] for a in gp["actions"]][:2] == ["generate-qr-code", "deeplink-redirect"], (st, gp))
st, out = call("POST", "/simulate/pay/INV-2026-00011?status=deny", b"")
check("a denied payment: deny, status_code 202", st == 200 and got[-1][1]["transaction_status"] == "deny" and got[-1][1]["status_code"] == "202", (st, out))

# a notification for an order the mock never saw, and one with a chosen status code (signed as sent)
st, out = call("POST", "/simulate/notify", {"order_id": "INV-2026-00012", "gross_amount": 99000, "payment_type": "shopeepay"})
check("/simulate/notify: settlement of an unseen order, signed", st == 200 and got[-1][0] and got[-1][1]["order_id"] == "INV-2026-00012"
      and got[-1][1]["payment_type"] == "shopeepay", (st, out))
st, out = call("POST", "/simulate/notify", {"order_id": "INV-2026-00013", "gross_amount": 99000, "transaction_status": "capture",
                                             "payment_type": "credit_card", "fraud_status": "challenge"})
check("/simulate/notify with fraud_status challenge", st == 200 and got[-1][0] and got[-1][1]["fraud_status"] == "challenge", (st, out))

# the mock signs anything it is asked: dummy keys only, from the environment, no key in /calls, escaped pages
st, calls = call("GET", "/calls")
check("/calls keeps requests without the Authorization header", st == 200 and calls and all("authorization" not in {k.lower() for k in c["headers"]} for c in calls)
      and KEY not in json.dumps(calls), "")
check("only dummy keys", m.dummy_key(KEY) and m.dummy_key("SB-Mid-server-TEST-x") and not m.dummy_key("SB-Mid-server-abc123") and not m.dummy_key("Mid-server-latest")
      and not m.dummy_key(""), KEY)
env = dict({k: v for k, v in os.environ.items() if k != "MIDTRANS_MOCK_SERVER_KEY"}, MIDTRANS_MOCK_SERVER_KEY="Mid-server-abc123")
r = subprocess.run([sys.executable, m.__file__, "--port", "0", "--notify-url", notify_url], capture_output=True, text=True, env=env, timeout=10)
check("the mock refuses to start with a key that is not a dummy", r.returncode == 2 and "dummy" in r.stderr, (r.returncode, r.stderr[-200:]))
st, page = call("GET", "/snap/v3/redirection/" + snap["token"])
check("the payment page names the order and escapes it", st == 200 and "INV-2026-00007" in page and "<script" not in page, page[:200])

print(f"midtrans mock: {len(fails)} failed")
sys.exit(1 if fails else 0)
