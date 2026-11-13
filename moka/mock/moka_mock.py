#!/usr/bin/env python3
"""A local stand-in for the Moka Connect API, and a poller that does what the kernel's poll trigger does, for demos and
tests without Moka API access (Moka grants it per partner; see moka/connector.rk).

    python3 moka/mock/moka_mock.py serve --port 8911 --fixtures moka/fixtures [--token demo-token] [--day 2026-11-04]
    python3 moka/mock/moka_mock.py poll --moka http://127.0.0.1:8911 --rangka http://127.0.0.1:8101 \
        [--today 2026-11-06]          # the Rangka API token comes from RANGKA_TOKEN (not argv, which `ps` shows)

serve answers, in Moka's formats, from the fixtures folder (latest_transactions.json, list_outlets.json):
- GET /v1/businesses/outlets                                  -> list_outlets.json
- GET /v3/outlets/<id>/reports/get_latest_transactions        -> that outlet's payments whose synchronized_at (or
  created_at / updated_at, time_filter) is at or after `since` (epoch seconds), newest first (reorder_type DESC, the
  default) or oldest first (ASC), `per_page` per page, the next page in data.next_url (null on the last page)
- GET /calls                                                  -> every request it received (for tests)
A request without `Authorization: Bearer <token>` is 401, as Moka answers. --day D moves every transaction to day D
(created_at, synchronized_at, refunded_date and the refunds keep their time of day; a refund made a later day than its
sale stays that many days later), so one fixture serves any demo date.

poll reads the mock the way the kernel's trigger moka.transaction_created does (contract 0002 §A.1, moka/connector.rk):
list_outlets first, then per outlet get_latest_transactions since two days before today with time_filter
synchronized_at, DESC, following next_url until it is null, and for each payment the trigger's emit mapping. Each event
goes to the tenant's flow moka_transaction_staged (POST /api/v1/flows/moka_transaction_staged/run), which is where the
kernel would start it. The kernel's own drop of a repeated event (hash kept 30 days) is not reproduced: the flow's
idempotency key (transaction and refunded quantity) is what keeps a repeated poll from booking twice, and the demo
polls twice to show it.

Why not let the kernel poll the mock: connector actions call https://api.mokapos.com (fixed in connector.rk) and the
kernel has no per-host override; a mock on 127.0.0.1 would need one. Until then the poller stands in for the kernel's
poll job, and only the HTTP leg between them is skipped.

Synthetic data only. Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WIB = timezone(timedelta(hours=7))


def parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def shift_day(s, base: date, day: date):
    """s moved from its sale day `base` to `day`, keeping the time of day and the offset (and any days after the sale)."""
    if not s:
        return s
    t = parse_ts(s)
    return (t + timedelta(days=(day - base).days)).isoformat()


def load(fixtures: str, day: str | None):
    pays = json.load(open(os.path.join(fixtures, "latest_transactions.json")))["data"]["payments"]
    outlets = json.load(open(os.path.join(fixtures, "list_outlets.json")))
    if day:
        d = date.fromisoformat(day)
        moved = []
        for p in pays:
            base = parse_ts(p["created_at"]).date()
            q = dict(p)
            for k in ("created_at", "synchronized_at", "updated_at", "refunded_date"):
                if q.get(k):
                    q[k] = shift_day(q[k], base, d)
            q["payment_refunds"] = [dict(r, created_at=shift_day(r.get("created_at"), base, d)) for r in (p.get("payment_refunds") or [])]
            moved.append(q)
        pays = moved
    return pays, outlets


class Mock:
    def __init__(self, fixtures: str, token: str, day: str | None = None):
        self.payments, self.outlets = load(fixtures, day)
        self.token = token
        self.calls: list[dict] = []
        self.lock = threading.Lock()

    def latest(self, outlet_id: str, q: dict, base: str) -> dict:
        since = int(q.get("since", ["0"])[0] or 0)
        field = q.get("time_filter", ["synchronized_at"])[0]
        if field not in ("created_at", "updated_at", "synchronized_at"):
            field = "synchronized_at"
        order = q.get("reorder_type", ["DESC"])[0].upper()
        per = max(1, min(100, int(q.get("per_page", ["100"])[0] or 100)))
        page = max(1, int(q.get("page", ["1"])[0] or 1))

        def when(p):
            return parse_ts(p.get(field) or p["created_at"]).timestamp()

        rows = [p for p in self.payments if str(p["outlet_id"]) == str(outlet_id) and when(p) >= since]
        rows.sort(key=when, reverse=order != "ASC")
        chunk = rows[(page - 1) * per: page * per]
        nxt = None
        if page * per < len(rows):
            qq = {k: v[0] for k, v in q.items()}
            qq["page"] = str(page + 1)
            nxt = f"{base}/v3/outlets/{outlet_id}/reports/get_latest_transactions?{urllib.parse.urlencode(qq)}"
        return {"data": {"completed": nxt is None, "next_url": nxt, "payments": chunk}}


def handler(m: Mock):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, code, obj):
            b = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self):
            u = urllib.parse.urlsplit(self.path)
            with m.lock:
                m.calls.append({"path": u.path, "query": u.query})
            if u.path == "/calls":
                return self.send(200, m.calls)
            if self.headers.get("Authorization") != f"Bearer {m.token}":
                return self.send(401, {"meta": {"code": 401, "error_type": "unauthorized", "error_message": "invalid access token"}})
            if u.path == "/v1/businesses/outlets":
                return self.send(200, m.outlets)
            mt = re.fullmatch(r"/v3/outlets/([^/]+)/reports/get_latest_transactions", u.path)
            if mt:
                base = f"http://{self.headers.get('Host')}"
                return self.send(200, m.latest(mt.group(1), urllib.parse.parse_qs(u.query), base))
            self.send(404, {"meta": {"code": 404, "error_message": "not found"}})

    return H


def get(url: str, token: str) -> dict:
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def emit(p: dict, outlet: dict) -> dict:
    """The emit mapping of trigger transaction_created (moka/connector.rk), item = p, outlet = the list_outlets row."""
    return {
        "transaction_id": p.get("id"), "outlet_id": p.get("outlet_id"), "outlet_name": outlet.get("name"),
        "receipt_no": p.get("payment_no"), "created_at": p.get("created_at"), "synchronized_at": p.get("synchronized_at"),
        "is_refunded": p.get("is_refunded"), "refunded_date": p.get("refunded_date"), "payment_refunds": p.get("payment_refunds"),
        "checkouts": p.get("checkouts"), "payment_type": p.get("payment_type"), "total_collected": p.get("total_collected"),
    }


def poll(moka: str, moka_token: str, rangka: str, rangka_token: str, today: str, max_pages: int = 50) -> dict:
    """One poll as the kernel runs it; returns {events, delivered, refused, pages}."""
    since = int((datetime.combine(date.fromisoformat(today), datetime.min.time(), WIB) - timedelta(days=2)).timestamp())
    out = {"events": 0, "delivered": 0, "refused": [], "pages": 0}
    for o in get(f"{moka}/v1/businesses/outlets", moka_token).get("data", []):
        url = f"{moka}/v3/outlets/{o['id']}/reports/get_latest_transactions?" + urllib.parse.urlencode(
            {"since": since, "time_filter": "synchronized_at", "reorder_type": "DESC", "per_page": "100"})
        for _ in range(max_pages):
            page = get(url, moka_token)["data"]
            out["pages"] += 1
            for p in page.get("payments") or []:
                out["events"] += 1
                body = json.dumps({"event": emit(p, o)}).encode()
                req = urllib.request.Request(f"{rangka}/api/v1/flows/moka_transaction_staged/run", data=body, method="POST",
                                             headers={"Content-Type": "application/json", "Authorization": f"Bearer {rangka_token}"})
                try:
                    with urllib.request.urlopen(req, timeout=60) as r:
                        out["delivered"] += r.status == 200
                except urllib.error.HTTPError as e:
                    out["refused"].append({"transaction": p.get("id"), "status": e.code, "body": e.read()[:300].decode(errors="replace")})
            url = page.get("next_url")
            if not url:
                break
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=8911)
    s.add_argument("--fixtures", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "fixtures"))
    s.add_argument("--token", default=os.environ.get("MOKA_MOCK_TOKEN", "demo-moka-token"))
    s.add_argument("--day", default=None, help="move every transaction to this day (YYYY-MM-DD)")
    p = sub.add_parser("poll")
    p.add_argument("--moka", required=True)
    p.add_argument("--rangka", required=True)
    p.add_argument("--today", default=datetime.now(WIB).date().isoformat())
    p.add_argument("--moka-token", default=os.environ.get("MOKA_MOCK_TOKEN", "demo-moka-token"))
    a = ap.parse_args(argv)
    if a.cmd == "serve":
        srv = ThreadingHTTPServer(("127.0.0.1", a.port), handler(Mock(a.fixtures, a.token, a.day)))
        print(f"moka mock: http://127.0.0.1:{srv.server_address[1]}", flush=True)
        srv.serve_forever()
    else:
        tok = os.environ.get("RANGKA_TOKEN", "")
        if not tok:
            sys.exit("set RANGKA_TOKEN (an API token of the tenant's owner or head office)")
        print(json.dumps(poll(a.moka, a.moka_token, a.rangka, tok, a.today)))


if __name__ == "__main__":
    main()
