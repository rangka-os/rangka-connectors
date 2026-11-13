#!/usr/bin/env python3
"""Check doku/mock/doku_mock.py: its signature equals the vector rangka-os checks the kernel against, Checkout refuses
a bad signature and answers a good one in DOKU's shape, and a simulated payment reaches the notify URL signed so that
the receiver recomputes it from the request it got. Stdlib only, no network beyond 127.0.0.1.

    python3 tests/check_doku_mock.py
"""
import json
import os
import sys
import threading
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "doku", "mock"))
import doku_mock as m  # noqa: E402

CLIENT, SECRET = "BRN-0001-CONTOH", "SK-test-dummy-0002"
fails = []


def check(name, cond, detail=""):
    print(("ok    " if cond else "FAIL  ") + name + ("" if cond else f"  {detail}"))
    if not cond:
        fails.append(name)


# 1. the vector of rangka-os kernel/internal/connectors/sign_test.go
body = b'{"order":{"amount":20000,"invoice_number":"STRK-DGO-2611-00001"},"payment":{"payment_due_date":60,"payment_method_types":["QRIS"]}}'
rid, ts = "11111111-2222-4333-8444-555555555555", "2026-11-11T05:00:00Z"
check("POST signature = kernel vector", m.sign_lines(SECRET, CLIENT, rid, ts, "/checkout/v1/payment", body) == "HMACSHA256=MENE8l4bupOlVvSWhuq+8lgjIRUobBxpRfz+HUTimJQ=")
check("GET signature (no Digest) = kernel vector", m.sign_lines(SECRET, CLIENT, rid, ts, "/orders/v1/status/INV-1", None) == "HMACSHA256=HtKPD0WkvZGTQX6XWFOnqcFPclr+UTDw6CTrqAdaHVQ=")

# 2. a receiver standing in for the kernel hook: verifies what it gets
got = []


class Hook(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        h = self.headers
        ok = h.get("Signature") == m.sign_lines(SECRET, h.get("Client-Id"), h.get("Request-Id"), h.get("Request-Timestamp"), self.path, raw)
        got.append((ok, json.loads(raw)))
        self.send_response(200 if ok else 401)
        self.end_headers()


hook = ThreadingHTTPServer(("127.0.0.1", 0), Hook)
threading.Thread(target=hook.serve_forever, daemon=True).start()
notify_url = f"http://127.0.0.1:{hook.server_address[1]}/hooks/tahu_contoh/doku/payment_notified"
mock = ThreadingHTTPServer(("127.0.0.1", 0), m.handler(m.State(CLIENT, SECRET, notify_url)))
threading.Thread(target=mock.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{mock.server_address[1]}"


def post(path, data: bytes, headers=None):
    req = urllib.request.Request(base + path, data=data, method="POST", headers=dict({"Content-Type": "application/json"}, **(headers or {})))
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


order = json.dumps({"order": {"amount": 750000, "invoice_number": "INV/TC/26/0007"}, "payment": {"payment_due_date": 30, "payment_method_types": ["QRIS"]}}).encode()
rid = str(uuid.uuid4())
hdr = {"Client-Id": CLIENT, "Request-Id": rid, "Request-Timestamp": ts}
st, out = post("/checkout/v1/payment", order, dict(hdr, Signature="HMACSHA256=bm9wZQ=="))
check("Checkout refuses a bad signature (401 invalid_signature)", st == 401 and out["error"]["code"] == "invalid_signature", (st, out))
st, out = post("/checkout/v1/payment", order, dict(hdr, Signature=m.sign_lines(SECRET, CLIENT, rid, ts, "/checkout/v1/payment", order)))
pay = (out or {}).get("response", {}).get("payment", {})
check("Checkout answers in DOKU's shape", st == 200 and pay.get("url", "").endswith(pay.get("token_id", "?")) and out["response"]["order"]["invoice_number"] == "INV/TC/26/0007", (st, out))

st, out = post(f"/simulate/pay/{pay.get('token_id')}", b"")
check("simulated payment notifies, receiver verifies the signature", st == 200 and out["notified"] == 200 and got and got[-1][0], (st, out, got))
n = got[-1][1] if got else {}
check("notification carries the invoice, amount, SUCCESS and the Checkout request id",
      n.get("order") == {"invoice_number": "INV/TC/26/0007", "amount": 750000} and n["transaction"]["status"] == "SUCCESS"
      and n["transaction"]["original_request_id"] == rid, n)
st, out = post("/simulate/notify", json.dumps({"invoice_number": "INV/TC/26/0008", "amount": 120000, "status": "FAILED"}).encode())
check("a FAILED notification is sent and verifies", st == 200 and got[-1][0] and got[-1][1]["transaction"]["status"] == "FAILED", (st, got[-1:]))

st, out = post(f"/simulate/refund/{pay.get('token_id')}?amount=250000", b"")
r = got[-1][1] if got else {}
check("a refund of the paid Checkout: signed notification, type REFUND, status SUCCESS, same request id, the refunded amount",
      st == 200 and out["notified"] == 200 and got[-1][0] and r["transaction"]["type"] == "REFUND" and r["transaction"]["status"] == "SUCCESS"
      and r["transaction"]["original_request_id"] == rid and r["order"]["amount"] == 250000, (st, out, r))
st, out = post("/simulate/refund/tok-unknown", b"")
check("refund of an unknown or unpaid Checkout: 404", st == 404, (st, out))
st, out = post("/simulate/notify", json.dumps({"invoice_number": "INV/TC/26/0008", "amount": 120000, "type": "REFUND"}).encode())
check("/simulate/notify with type REFUND", st == 200 and got[-1][1]["transaction"]["type"] == "REFUND", got[-1:])

# the mock signs anything it is asked: it takes dummy keys only, from the environment, and escapes what it echoes
check("only dummy keys", m.dummy_key(SECRET) and m.dummy_key("SK-TEST-x") and not m.dummy_key("SK-backoffice-key") and not m.dummy_key("SK-attestation-key")
      and not m.dummy_key(""), SECRET)
import subprocess  # noqa: E402
env = {k: v for k, v in os.environ.items() if k != "DOKU_MOCK_SECRET_KEY"}
r = subprocess.run([sys.executable, m.__file__, "--port", "0", "--client-id", CLIENT, "--secret-key", "SK-backoffice-key",
                    "--notify-url", notify_url], capture_output=True, text=True, env=env, timeout=10)
check("the mock refuses to start with a key that is not a dummy", r.returncode == 2 and "dummy" in r.stderr, (r.returncode, r.stderr[-200:]))
evil = json.dumps({"order": {"amount": 1000, "invoice_number": "<script>x</script>"}, "payment": {"payment_due_date": 30}}).encode()
rid2 = str(uuid.uuid4())
st, out = post("/checkout/v1/payment", evil, {"Client-Id": CLIENT, "Request-Id": rid2, "Request-Timestamp": ts,
                                              "Signature": m.sign_lines(SECRET, CLIENT, rid2, ts, "/checkout/v1/payment", evil)})
tok = (out or {}).get("response", {}).get("payment", {}).get("token_id", "")
page = urllib.request.urlopen(base + "/checkout-link-v2/" + tok, timeout=10).read().decode() if st == 200 else ""
check("the payment page escapes the invoice number", "<script>" not in page and "&lt;script&gt;" in page, page[:200])

print(f"doku mock: {len(fails)} failed")
sys.exit(1 if fails else 0)
