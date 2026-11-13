#!/usr/bin/env python3
"""Check moka/mock/moka_mock.py: Moka's answers come from the fixtures with Bearer auth, the `since` window, newest
first, pages through next_url; --day moves the sales and keeps a later refund later; poll lists the outlets, follows
every page and delivers one event per payment in the trigger's emit shape to the flow URL. Stdlib only, 127.0.0.1.

    python3 tests/check_moka_mock.py
"""
import json
import os
import sys
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "moka", "mock"))
import moka_mock as m  # noqa: E402

FIX = os.path.join(ROOT, "moka", "fixtures")
TOKEN = "demo-moka-token"
fails = []


def check(name, cond, detail=""):
    print(("ok    " if cond else "FAIL  ") + name + ("" if cond else f"  {detail}"))
    if not cond:
        fails.append(name)


def serve(handler):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def get(url, token=TOKEN):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


pays = json.load(open(os.path.join(FIX, "latest_transactions.json")))["data"]["payments"]
mock = m.Mock(FIX, TOKEN)
srv, base = serve(m.handler(mock))

# 1. auth and outlets
st, _ = get(f"{base}/v1/businesses/outlets", token="")
check("no Bearer token: 401", st == 401, st)
st, _ = get(f"{base}/v1/businesses/outlets", token="wrong")
check("wrong token: 401", st == 401, st)
st, outs = get(f"{base}/v1/businesses/outlets")
check("list_outlets answers the fixture", st == 200 and [o["id"] for o in outs["data"]] == [70001, 70002], outs)

# 2. latest transactions: window, order, pages
oid = pays[0]["outlet_id"]
mine = [p for p in pays if p["outlet_id"] == oid]
st, all_ = get(f"{base}/v3/outlets/{oid}/reports/get_latest_transactions?since=0&per_page=100")
times = [m.parse_ts(p["synchronized_at"]) for p in all_["data"]["payments"]]
check("since=0: every payment of the outlet, newest first, no next page",
      st == 200 and len(times) == len(mine) and times == sorted(times, reverse=True) and all_["data"]["next_url"] is None, all_)
st, asc = get(f"{base}/v3/outlets/{oid}/reports/get_latest_transactions?since=0&reorder_type=ASC")
check("reorder_type ASC: oldest first", [p["id"] for p in asc["data"]["payments"]] == [p["id"] for p in reversed(all_["data"]["payments"])], asc)
later = int(max(times).timestamp()) + 1
st, none = get(f"{base}/v3/outlets/{oid}/reports/get_latest_transactions?since={later}")
check("since after the last sync: no payments", st == 200 and none["data"]["payments"] == [], none)
if len(mine) > 1:
    st, p1 = get(f"{base}/v3/outlets/{oid}/reports/get_latest_transactions?since=0&per_page=1")
    nxt = p1["data"]["next_url"]
    st2, p2 = get(nxt) if nxt else (0, {})
    check("per_page=1: next_url leads to the second payment",
          nxt is not None and p2["data"]["payments"][0]["id"] == all_["data"]["payments"][1]["id"], (p1, p2))

# 3. --day moves the sale and keeps the refund's distance
moved = m.Mock(FIX, TOKEN, day="2026-11-04")
p0 = next(p for p in pays if p.get("refunded_date"))
q0 = next(p for p in moved.payments if p["id"] == p0["id"])
gap = lambda p: m.parse_ts(p["refunded_date"]) - m.parse_ts(p["created_at"])  # noqa: E731
check("--day: the sale moves to that day, time of day kept", q0["created_at"][:10] == "2026-11-04" and q0["created_at"][10:] == p0["created_at"][10:], q0["created_at"])
check("--day: the refund stays as far after the sale", gap(q0) == gap(p0) and q0["payment_refunds"][0]["created_at"][:10] >= "2026-11-04", q0)


# 4. poll: outlets -> pages -> one event per payment, emit shape, Bearer token of Rangka
got = []


class Flow(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        got.append({"path": self.path, "auth": self.headers.get("Authorization"), "body": json.loads(self.rfile.read(n))})
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")


fsrv, fbase = serve(Flow)
small = m.Mock(FIX, TOKEN, day="2026-11-04")
ssrv, sbase = serve(m.handler(small))
res = m.poll(sbase, TOKEN, fbase, "rk-token", "2026-11-05")
check("poll: one event per payment of every outlet, all delivered", res["events"] == len(pays) == res["delivered"] and not res["refused"], res)
check("poll: posts to flow moka_transaction_staged with the Rangka token",
      all(g["path"] == "/api/v1/flows/moka_transaction_staged/run" and g["auth"] == "Bearer rk-token" for g in got), got[:1])
ev = got[0]["body"]["event"] if got else {}
want = {"transaction_id", "outlet_id", "outlet_name", "receipt_no", "created_at", "synchronized_at", "is_refunded", "refunded_date",
        "payment_refunds", "checkouts", "payment_type", "total_collected"}
check("poll: event carries exactly the trigger's emit fields", set(ev) == want, sorted(ev))
check("poll: outlet_name from list_outlets", all(g["body"]["event"]["outlet_name"].startswith("Tahu Contoh") for g in got), [g["body"]["event"]["outlet_name"] for g in got])
calls = [c["path"] for c in small.calls]
check("poll: list_outlets first, then latest transactions per outlet", calls[0] == "/v1/businesses/outlets" and sum("get_latest_transactions" in c for c in calls) == 2, calls)
got.clear()
res = m.poll(sbase, TOKEN, fbase, "rk-token", "2026-11-09")
check("poll five days later: the two-day window holds nothing", res["events"] == 0 and not got, res)

print(f"{'FAIL' if fails else 'ok'}: {len(fails)} failed")
sys.exit(1 if fails else 0)
