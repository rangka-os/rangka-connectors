#!/usr/bin/env python3
"""Check xendit/mock/xendit_mock.py: Basic auth on the API, one VA per external_id, a closed VA takes only its amount,
the VA and QRIS callbacks carry x-callback-token and every field connector `xendit` emits from, a single-use VA and a
dynamic QR are paid once, a redelivered callback is the same payment (same id) and can carry another token, and the
mock refuses a key that is not a dummy. Stdlib only, no network beyond 127.0.0.1.

    python3 tests/check_xendit_mock.py
"""
import base64
import json
import os
import re
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "xendit", "mock"))
import xendit_mock as m  # noqa: E402

SECRET, TOKEN = "xnd_development_dummy_check", "dummy-cb-check"
fails = []


def check(name, cond, detail=""):
    print(("ok    " if cond else "FAIL  ") + name + ("" if cond else f"  {detail}"))
    if not cond:
        fails.append(name)


got = []


class Hook(BaseHTTPRequestHandler):
    """The receiver (Juragan's shim or a kernel hook): accepts only the right token."""

    def log_message(self, *a):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        ok = self.headers.get("x-callback-token") == TOKEN
        got.append((self.path, ok, json.loads(raw)))
        self.send_response(200 if ok else 401)
        self.end_headers()


hook = ThreadingHTTPServer(("127.0.0.1", 0), Hook)
threading.Thread(target=hook.serve_forever, daemon=True).start()
hb = f"http://127.0.0.1:{hook.server_address[1]}"
mock = ThreadingHTTPServer(("127.0.0.1", 0), m.handler(m.State(SECRET, TOKEN, hb + "/va_paid", hb + "/qr_paid")))
threading.Thread(target=mock.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{mock.server_address[1]}"
AUTH = {"Authorization": "Basic " + base64.b64encode((SECRET + ":").encode()).decode()}


def post(path, body, headers=None):
    req = urllib.request.Request(base + path, data=json.dumps(body).encode(), method="POST", headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


# the fields connector.rk reads from each callback (emit), so the mock and the connector stay in step
rk = open(os.path.join(ROOT, "xendit", "connector.rk"), encoding="utf8").read()


def emitted(trigger):
    block = rk.split(f"- name: {trigger}", 1)[1].split("\n  - name:", 1)[0].split("emit:", 1)[1]
    paths = set()
    for line in block.splitlines():
        mm = re.match(r"\s+\w+:\s*(.+)$", line)
        if mm:
            for p in re.findall(r"(?<!['\w.])([a-z_]+(?:\.[a-z_]+)*)", mm.group(1).split("#")[0]):
                if p not in {"true", "false"}:
                    paths.add(p)
    return paths


def has(payload, path):
    cur = payload
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return False
        cur = cur[k]
    return True


# 1. auth and one VA per invoice
st, _ = post("/callback_virtual_accounts", {"external_id": "JRG/2026/11/0001", "bank_code": "BCA", "name": "Kedai", "expected_amount": 250000})
check("VA without the secret key -> 401", st == 401, st)
st, va = post("/callback_virtual_accounts", {"external_id": "JRG/2026/11/0001", "bank_code": "BCA", "name": "Kedai", "expected_amount": 250000, "is_closed": True, "is_single_use": True}, AUTH)
check("VA created: BCA number, closed, PENDING", st == 200 and va["account_number"].startswith("39108") and va["is_closed"] and va["status"] == "PENDING", (st, va))
st, va2 = post("/callback_virtual_accounts", {"external_id": "JRG/2026/11/0001", "bank_code": "BCA", "name": "Kedai", "expected_amount": 250000}, AUTH)
check("same external_id again -> the same VA (a retried request makes no second VA)", st == 200 and va2["id"] == va["id"], (va2, va))

# 2. pay the VA: wrong amount refused, right amount -> callback with token and every emitted field
st, r = post("/callback_virtual_accounts/external_id=JRG%2F2026%2F11%2F0001/simulate_payment", {"amount": 100000})
check("closed VA, another amount -> 400 INVALID_AMOUNT", st == 400 and r.get("error_code") == "INVALID_AMOUNT", (st, r))
st, r = post("/callback_virtual_accounts/external_id=JRG%2F2026%2F11%2F0001/simulate_payment", {"amount": 250000})
check("VA paid -> callback delivered 200", st == 200 and r["callback_status"] == 200, (st, r))
path, ok, cb = got[-1]
check("VA callback: to va_paid with the right token", path == "/va_paid" and ok, (path, ok))
need = emitted("va_paid") - {"auth"}
check(f"VA callback has every field connector va_paid emits ({', '.join(sorted(need))})", all(has(cb, p) for p in need), [p for p in need if not has(cb, p)])
check("VA callback: external_id and amount of the invoice", cb["external_id"] == "JRG/2026/11/0001" and cb["amount"] == 250000, cb)
st, r2 = post("/callback_virtual_accounts/external_id=JRG%2F2026%2F11%2F0001/simulate_payment", {"amount": 250000})
check("single-use VA paid a second time -> 400", st == 400, (st, r2))

# 3. redelivery: the same payment again (idempotency is the receiver's), and with another token
st, rd = post(f"/simulate/redeliver/{r['payment_id']}", {})
check("redelivered callback: same payment_id", st == 200 and got[-1][2]["payment_id"] == cb["payment_id"] and got[-1][1], got[-1])
st, rd = post(f"/simulate/redeliver/{r['payment_id']}?token=dummy-wrong", {})
check("redelivered with another token -> the receiver refuses it (401)", rd["callback_status"] == 401 and not got[-1][1], (rd, got[-1][1]))

# 4. QRIS: create, pay once, the qr.payment callback
st, qr = post("/qr_codes", {"reference_id": "JRG/2026/12/0002", "type": "DYNAMIC", "currency": "IDR", "amount": 250000}, AUTH)
check("QR created: ACTIVE with a qr_string", st == 201 and qr["status"] == "ACTIVE" and qr["qr_string"], (st, qr))
st, r = post(f"/qr_codes/{qr['id']}/payments/simulate", {"amount": 250000})
path, ok, cb = got[-1]
check("QR paid -> qr_paid callback with the right token", st == 200 and r["callback_status"] == 200 and path == "/qr_paid" and ok, (st, r, path))
need = emitted("qr_paid") - {"auth"}
check(f"QR callback has every field connector qr_paid emits ({', '.join(sorted(need))})", all(has(cb, p) for p in need), [p for p in need if not has(cb, p)])
check("QR callback: event qr.payment, status SUCCEEDED, reference_id = invoice", cb["event"] == "qr.payment" and cb["data"]["status"] == "SUCCEEDED" and cb["data"]["reference_id"] == "JRG/2026/12/0002", cb)
st, _ = post(f"/qr_codes/{qr['id']}/payments/simulate", {"amount": 250000})
check("a dynamic QR paid a second time -> 400", st == 400, st)

# 5. /calls redacts the key; a real-looking key is refused at start
calls = json.loads(urllib.request.urlopen(base + "/calls").read())
check("/calls never shows the Authorization header", all(c["headers"].get("Authorization", "[redacted]") == "[redacted]" for c in calls), calls[:1])
for key, tok in (("xnd_production_abc123", "dummy-cb"), ("xnd_development_abc123", "dummy-cb"), ("xnd_development_dummy_x", "realtoken123")):
    os.environ["XENDIT_MOCK_SECRET_KEY"], os.environ["XENDIT_MOCK_CALLBACK_TOKEN"] = key, tok
    try:
        m.main(["--port", "0"])
        refused = False
    except SystemExit as e:
        refused = e.code == 2
    check(f"start refused for key {key[:16]}... / token {tok}", refused)

print(f"\n{'ALL OK' if not fails else f'{len(fails)} FAILED'}")
sys.exit(1 if fails else 0)
