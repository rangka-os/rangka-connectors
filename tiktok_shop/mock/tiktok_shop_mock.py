#!/usr/bin/env python3
"""A local stand-in for the TikTok Shop Partner API (which also carries Tokopedia orders), and a poll relay that does
what the kernel's poll triggers will do once it can sign in the query string (tiktok_shop/connector.rk, Juragan's
proposals/marketplace-connector-runtime.md). For demos and tests without a TikTok Shop app.

    TIKTOK_SHOP_MOCK_APP_SECRET=tts-test-dummy-secret python3 tiktok_shop/mock/tiktok_shop_mock.py serve \
        --port 8931 --app-key demo-app-key [--webhook-url http://127.0.0.1:8080/hooks/<org>/tiktok_shop/webhook_received]

    TIKTOK_SHOP_APP_KEY=demo-app-key TIKTOK_SHOP_APP_SECRET=tts-test-dummy-secret \
    TIKTOK_SHOP_SHOP_CIPHER=ROW_TahuContohDemoCipher0001 TIKTOK_SHOP_ACCESS_TOKEN=... TIKTOK_SHOP_REFRESH_TOKEN=... \
    RANGKA_TOKEN=... python3 tiktok_shop/mock/tiktok_shop_mock.py poll --api http://127.0.0.1:8931 \
        --auth-api http://127.0.0.1:8931 --rangka http://127.0.0.1:8101 --token-file tts-tokens.json [--dry-run] \
        [--since-days 2] [--settled-days 7]

Dummy keys only. The mock signs any webhook it is asked for (/simulate/*), without authentication: it is a signing
oracle for its secret. So it listens on 127.0.0.1 only, reads the secret from TIKTOK_SHOP_MOCK_APP_SECRET (not argv,
which `ps` shows), and refuses a secret that does not say it is a dummy (a part "dummy" or "test" between - _ or .).
It never prints a secret or a token.

serve answers in TikTok Shop's formats, from the fixtures folder and in-memory state. Every call to the API paths
checks, in this order, as TikTok documents: app_key (36009004), timestamp within 5 minutes before to 30 seconds after
the mock's clock (36009004), sign present (36009004) and equal to sign_request() (106001), x-tts-access-token known
(101000) and not expired (105002), shop_cipher present (106013) and the shop's (101000). Errors use TikTok's envelope
{"code", "message", "request_id", "data"}.
- GET  /api/v2/token/get?app_key&app_secret&auth_code&grant_type=authorized_code   (not signed, as at TikTok)
- GET  /api/v2/token/refresh?app_key&app_secret&refresh_token&grant_type=refresh_token
       A refresh rotates both tokens; the old refresh token stops working (the strictest reading: TikTok does not say
       whether it rotates, so a client must store whatever comes back).
- GET  /authorization/202309/shops                         list_shops.json (no shop_cipher on this call)
- POST /order/202309/orders/search?page_size&page_token&sort_field&sort_order   body filters update_time_ge/_lt,
       create_time_ge/_lt, order_status; page_token paging, next_page_token "" on the last page
- GET  /order/202507/orders?ids=a,b                        order detail (max 50)
- GET  /finance/202309/statements?statement_time_ge&_lt&payment_status&page_size&page_token
- GET  /finance/202501/statements/<id>/statement_transactions?page_size&page_token
- POST /product/202309/products/<product_id>/inventory/update   sets the absolute quantity (kept in memory)
- POST /product/202502/products/search                      products with their current inventory; seller_skus filter
- GET  /fulfillment/202309/packages/<package_id>            tracking number of a package
- GET  /calls                                               every request it received (for tests; tokens masked)
/simulate/* (no auth, POST): create (body {"commerce_platform", "skus": [seller_sku, ...]}, one line per unit),
pay/<id>, hold/<id>, ship/<id>, deliver/<id>, complete/<id> (also makes its paid statement and transaction),
cancel/<id>, expire_token, authorize (an auth code for /api/v2/token/get). Each order change bumps update_time and,
with --webhook-url, sends webhook type 1 signed as TikTok signs it: Authorization = hex HMAC-SHA256(app_secret,
app_key + raw body).

Orders never carry buyer data here: recipient_address, buyer_nickname, buyer_email, buyer_message and user_id are
dropped from every fixture before it is served. Fixture times are moved by whole days so that the latest one falls in
the last 24 hours (--keep-times turns this off), so the two-day poll window always finds them.

poll reads the API the way the kernel's triggers order_changed and order_settled will: it signs every request, pages
over next_page_token (orders, statements and statement transactions), refreshes once on 105002 and sends the call
again, reads the paid statements and their transactions, builds each event with exactly the keys the triggers emit,
and posts it to the tenant's flow tiktok_shop_order_staged or tiktok_shop_order_settled (POST
/api/v1/flows/<name>/run with RANGKA_TOKEN), as moka_mock.py poll does. A rotated token pair is written to
--token-file (mode 0600) whenever a refresh happened, also on --dry-run: TikTok has already rotated it, and losing it
means the owner must authorize again. A run that posts refuses to start without --token-file.
The app secret and tokens go only over https to the hosts of connector.rk (open-api.tiktokglobalshop.com,
auth.tiktok-shops.com), or plain http to 127.0.0.1 or localhost (the mock), for --api and --auth-api alike;
RANGKA_TOKEN goes only over https, or plain http to 127.0.0.1 or localhost. Both are checked before any request.
Each poll reads a fixed window and keeps no cursor: after an outage longer than the window (relay off, kernel down,
token expired) run poll --since-days N --settled-days N once, N covering the outage. The flows' idempotency keys
(marketplace/README.md, "Flow keys") make the overlap and the repeated events of every poll harmless.

Synthetic data only. Stdlib only.
"""
from __future__ import annotations

import argparse
import base64
import copy
import glob
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOSTS = ("open-api.tiktokglobalshop.com", "auth.tiktok-shops.com")   # hosts: of tiktok_shop/connector.rk
LOCAL = ("127.0.0.1", "localhost", "::1")
API_PREFIXES = ("/authorization/", "/order/", "/finance/", "/product/", "/fulfillment/")
BUYER_KEYS = ("recipient_address", "buyer_nickname", "buyer_email", "buyer_message", "user_id")
# the flag table of marketplace/README.md, column tiktok_shop (status of the order API)
SOLD = ("AWAITING_SHIPMENT", "PARTIALLY_SHIPPING", "AWAITING_COLLECTION", "IN_TRANSIT", "DELIVERED", "COMPLETED")
SHIPPED = ("IN_TRANSIT", "DELIVERED", "COMPLETED")
ACCESS_TTL, REFRESH_TTL = 7 * 86400, 365 * 86400
FLOW_STAGED, FLOW_SETTLED = "tiktok_shop_order_staged", "tiktok_shop_order_settled"


# ---------------------------------------------------------------------------------------------------------- signing
def sign_request(app_secret: str, path: str, query: dict, body: bytes | str | None = None) -> str:
    """TikTok Shop request signature: hex HMAC-SHA256(app_secret, app_secret + path + sorted key+value of the query
    without sign and access_token + raw body + app_secret). The body is left out for multipart/form-data (not used)."""
    if isinstance(body, str):
        body = body.encode()
    params = "".join(f"{k}{query[k]}" for k in sorted(query) if k not in ("sign", "access_token"))
    base = app_secret.encode() + path.encode() + params.encode() + (body or b"") + app_secret.encode()
    return hmac.new(app_secret.encode(), base, hashlib.sha256).hexdigest()


def sign_webhook(app_key: str, app_secret: str, body: bytes | str) -> str:
    """TikTok Shop webhook signature (the Authorization header): hex HMAC-SHA256(app_secret, app_key + raw body)."""
    if isinstance(body, str):
        body = body.encode()
    return hmac.new(app_secret.encode(), app_key.encode() + body, hashlib.sha256).hexdigest()


def dummy_key(secret: str) -> bool:
    """True for a secret that says it is a dummy: the mock signs whatever it is asked, so a real one must never reach it."""
    return bool(re.search(r"(^|[-_.])(dummy|test)([-_.]|$)", secret or "", re.IGNORECASE))


# ------------------------------------------------------------------------------------------------------- the events
def order_event(o: dict) -> dict:
    """The emit of trigger order_changed (tiktok_shop/connector.rk), item = o."""
    st = o.get("status")
    pay = o.get("payment") or {}
    return {
        "channel": "tiktok_shop", "storefront": o.get("commerce_platform"), "order_id": o.get("id"), "status": st,
        "sold": st in SOLD, "shipped": st in SHIPPED, "completed": st == "COMPLETED", "cancelled": st == "CANCELLED",
        "created_at": o.get("create_time"), "paid_at": o.get("paid_time"), "updated_at": o.get("update_time"),
        "currency": pay.get("currency"), "buyer_total": pay.get("total_amount"), "lines": o.get("line_items"),
    }


def settled_event(statement: dict, t: dict) -> dict:
    """The emit of trigger order_settled, statement = the list_settlements row, item = t."""
    ftb = t.get("fee_tax_breakdown") or {}
    fee, tax = ftb.get("fee") or {}, ftb.get("tax") or {}
    return {
        "channel": "tiktok_shop", "order_id": t.get("order_id"), "currency": statement.get("currency"),
        "settled_at": statement.get("payment_time"), "net": t.get("settlement_amount"), "adjusted_net": None,
        "commission_fee": fee.get("platform_commission_amount"), "service_fee": None,
        "transaction_fee": fee.get("transaction_fee_amount"), "withheld_tax": tax.get("article_22_income_tax_withheld"),
        "income": t,
    }


# ---------------------------------------------------------------------------------------------------------- the mock
TIME_KEYS = ("create_time", "update_time", "paid_time", "rts_time", "rts_sla_time", "cancel_time", "collection_time",
             "delivery_time", "statement_time", "payment_time", "order_create_time")


def shift_times(obj, delta: int):
    if isinstance(obj, dict):
        return {k: (v + delta if k in TIME_KEYS and isinstance(v, int) else shift_times(v, delta)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [shift_times(v, delta) for v in obj]
    return obj


def scrub(o: dict) -> dict:
    return {k: v for k, v in o.items() if k not in BUYER_KEYS}


class State:
    """The mock's memory. app_secret is a dummy: whoever reaches the mock can have anything signed with it."""

    def __init__(self, fixtures: str, app_key: str, app_secret: str, access_token: str, refresh_token: str,
                 webhook_url: str | None = None, keep_times: bool = False, max_page_size: int = 100, clock=time.time):
        self.app_key, self.app_secret, self.webhook_url, self.clock = app_key, app_secret, webhook_url, clock
        self.max_page_size = max(1, min(100, max_page_size))
        self.lock = threading.Lock()
        self.calls: list[dict] = []
        now = int(clock())
        self.access = {access_token: now + ACCESS_TTL}
        self.refresh = {refresh_token: now + REFRESH_TTL}
        self.auth_codes: dict[str, int] = {}
        self.seq = 0

        def fx(name):
            return json.load(open(os.path.join(fixtures, name)))
        orders = [fx(os.path.basename(f))["item"] for f in sorted(glob.glob(os.path.join(fixtures, "order_changed*.json")))]
        settled = fx("order_settled.json")
        self.shops = fx("list_shops.json")["data"]["shops"]
        self.shop = self.shops[0]
        products = fx("list_products.json")["data"]["products"]
        pkg = fx("get_tracking.json")["data"]
        if not keep_times:
            latest = max([o.get("update_time") or 0 for o in orders] + [settled["statement"].get("payment_time") or 0])
            delta = ((now - latest) // 86400) * 86400
            orders, settled, products, pkg = (shift_times(x, delta) for x in (orders, settled, products, pkg))
        self.orders = {o["id"]: scrub(o) for o in orders}
        self.statements = {settled["statement"]["id"]: dict(settled["statement"])}
        self.transactions = {settled["statement"]["id"]: [settled["item"]]}
        self.products = {p["id"]: p for p in products}
        self.packages = {pkg["package_id"]: pkg}
        self.webhooks: list[dict] = []

    # -- helpers
    def new_id(self, prefix: str, width: int = 19) -> str:
        self.seq += 1
        return (prefix + str(int(self.clock())) + f"{self.seq:04d}").ljust(width, "0")[:width]

    def touch(self, o: dict):
        o["update_time"] = max(int(self.clock()), int(o.get("update_time") or 0) + 1)

    def set_status(self, o: dict, status: str, line_status: str | None = None, package_status: str | None = None):
        o["status"] = status
        o["is_on_hold_order"] = status == "ON_HOLD"
        for li in o.get("line_items") or []:
            li["display_status"] = line_status or status
            if package_status:
                li["package_status"] = package_status
        self.touch(o)

    def webhook(self, o: dict) -> int:
        """Webhook type 1 for an order change, signed as TikTok signs; returns the HTTP status (0 unreachable/none)."""
        status = "CANCEL" if o["status"] == "CANCELLED" else o["status"]
        body = json.dumps({"type": 1, "tts_notification_id": self.new_id("738"), "shop_id": self.shop["id"],
                           "timestamp": int(self.clock()), "data": {"order_id": o["id"], "order_status": status,
                                                                    "is_on_hold_order": o["status"] == "ON_HOLD",
                                                                    "update_time": o["update_time"]}},
                          separators=(",", ":")).encode()
        self.webhooks.append(json.loads(body))
        if not self.webhook_url:
            return 0
        req = urllib.request.Request(self.webhook_url, data=body, method="POST", headers={
            "Content-Type": "application/json", "Authorization": sign_webhook(self.app_key, self.app_secret, body)})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code
        except OSError:
            return 0

    def settle(self, o: dict):
        """A paid statement for today with one ORDER transaction, fees as negative strings as in TikTok's samples."""
        now = int(self.clock())
        revenue = sum(int(li.get("sale_price") or 0) for li in o.get("line_items") or [])
        commission, txfee, pph22 = revenue * 4 // 100, revenue * 2 // 100, revenue * 5 // 1000
        fee_tax = -(commission + txfee + pph22)
        net = revenue + fee_tax
        sid = self.new_id("730")
        tx = {"id": self.new_id("730"), "type": "ORDER", "order_id": o["id"], "order_create_time": o["create_time"],
              "adjustment_id": "", "adjustment_order_id": "", "adjustment_amount": "0",
              "settlement_amount": str(net), "revenue_amount": str(revenue),
              "revenue_breakdown": {"subtotal_before_discount_amount": str(revenue), "seller_discount_amount": "0"},
              "shipping_cost_amount": "0", "shipping_cost_breakdown": {"actual_shipping_fee_amount": "0"},
              "fee_tax_amount": str(fee_tax),
              "fee_tax_breakdown": {"fee": {"platform_commission_amount": str(-commission), "transaction_fee_amount": str(-txfee)},
                                    "tax": {"article_22_income_tax_withheld": str(-pph22)}},
              "supplementary_component": {"customer_payment_amount": (o.get("payment") or {}).get("total_amount", "0")}}
        self.statements[sid] = {"id": sid, "statement_time": now - now % 86400, "settlement_amount": str(net), "currency": "IDR",
                                "revenue_amount": str(revenue), "fee_amount": str(fee_tax), "adjustment_amount": "0",
                                "payment_status": "PAID", "payment_id": self.new_id("731"), "payment_time": now}
        self.transactions[sid] = [tx]


def envelope(code: int, message: str, data=None) -> dict:
    return {"code": code, "message": message, "request_id": secrets.token_hex(16).upper(), "data": data if data is not None else {}}


def page_of(rows: list, q: dict, cap: int):
    """TikTok-style paging: page_size from the query (capped by --max-page-size), an opaque page_token."""
    try:
        size = max(1, min(cap, int(q.get("page_size") or 20)))
    except ValueError:
        size = 20
    start = 0
    if q.get("page_token"):
        try:
            start = int(base64.urlsafe_b64decode(q["page_token"].encode()).decode().removeprefix("o:"))
        except (ValueError, UnicodeDecodeError):
            return None, None
    chunk = rows[start:start + size]
    nxt = base64.urlsafe_b64encode(f"o:{start + size}".encode()).decode() if start + size < len(rows) else ""
    return chunk, nxt


def handler(st: State):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def send(self, code: int, obj):
            raw = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def ok(self, data):
            self.send(200, envelope(0, "Success", data))

        def err(self, http: int, code: int, message: str):
            self.send(http, envelope(code, message))

        def record(self, method: str, raw: bytes):
            u = urllib.parse.urlsplit(self.path)
            q = dict(urllib.parse.parse_qsl(u.query, keep_blank_values=True))
            for k in ("app_secret", "refresh_token", "access_token", "auth_code"):
                if k in q:
                    q[k] = "***"
            with st.lock:
                st.calls.append({"method": method, "path": u.path, "query": q, "body": raw.decode(errors="replace"),
                                 "has_access_token": bool(self.headers.get("x-tts-access-token"))})

        def do_GET(self):
            self.route("GET", b"")

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            self.route("POST", self.rfile.read(n) if n else b"")

        def do_PUT(self):
            self.do_POST()

        # -- the API gate, in TikTok's order of checks
        def gate(self, path: str, q: dict, raw: bytes, shop_scoped: bool) -> bool:
            if q.get("app_key") != st.app_key:
                return self.err(401, 36009004, "Invalid app_key") or False
            try:
                ts = int(q.get("timestamp", ""))
            except ValueError:
                return self.err(401, 36009004, "Invalid timestamp") or False
            now = int(st.clock())
            if not (now - 300 <= ts <= now + 30):
                return self.err(401, 36009004, "Invalid timestamp: outside 5 minutes before to 30 seconds after the current time") or False
            if not q.get("sign"):
                return self.err(401, 36009004, "Missing signature") or False
            if not hmac.compare_digest(q["sign"], sign_request(st.app_secret, path, q, raw)):
                return self.err(401, 106001, "Invalid sign") or False
            tok = self.headers.get("x-tts-access-token") or ""
            with st.lock:
                exp = st.access.get(tok)
            if exp is None:
                return self.err(401, 101000, "Invalid x-tts-access-token") or False
            if exp <= now:
                return self.err(401, 105002, "Access token is expired, please refresh it") or False
            if shop_scoped:
                if not q.get("shop_cipher"):
                    return self.err(400, 106013, "Missing shop_cipher") or False
                if q["shop_cipher"] != st.shop["cipher"]:
                    return self.err(401, 101000, "Invalid shop_cipher") or False
            return True

        def route(self, method: str, raw: bytes):
            u = urllib.parse.urlsplit(self.path)
            path = u.path
            q = dict(urllib.parse.parse_qsl(u.query, keep_blank_values=True))
            self.record(method, raw)
            if path == "/calls" and method == "GET":
                with st.lock:
                    return self.send(200, st.calls)
            if path.startswith("/api/v2/token/"):
                return self.token(path, q)
            if path.startswith("/simulate/") and method == "POST":
                return self.simulate(path, raw)
            if not path.startswith(API_PREFIXES):
                return self.err(404, 36009009, "Invalid path")
            if not self.gate(path, q, raw, shop_scoped=path != "/authorization/202309/shops"):
                return
            try:
                body = json.loads(raw) if raw else {}
            except ValueError:
                return self.err(400, 36009022, "Invalid request format")
            return self.api(method, path, q, body)

        # -- tokens (auth.tiktok-shops.com, not signed)
        def token(self, path: str, q: dict):
            if q.get("app_key") != st.app_key or not hmac.compare_digest(q.get("app_secret", ""), st.app_secret):
                return self.err(401, 36009004, "Invalid app_key or app_secret")
            now = int(st.clock())
            with st.lock:
                if path == "/api/v2/token/get":
                    if q.get("grant_type") != "authorized_code":
                        return self.err(400, 36009004, "Invalid grant_type")
                    if st.auth_codes.pop(q.get("auth_code", ""), 0) < now:
                        return self.err(400, 36004004, "Invalid auth code")
                elif path == "/api/v2/token/refresh":
                    if q.get("grant_type") != "refresh_token":
                        return self.err(400, 36009004, "Invalid grant_type")
                    if st.refresh.pop(q.get("refresh_token", ""), 0) < now:
                        return self.err(400, 36009004, "Invalid refresh_token")
                else:
                    return self.err(404, 36009009, "Invalid path")
                access, refresh = "tts-access-" + secrets.token_hex(12), "tts-refresh-" + secrets.token_hex(12)
                st.access[access] = now + ACCESS_TTL
                st.refresh[refresh] = now + REFRESH_TTL
            return self.ok({"access_token": access, "access_token_expire_in": now + ACCESS_TTL, "refresh_token": refresh,
                            "refresh_token_expire_in": now + REFRESH_TTL, "open_id": "mock-open-id", "seller_name": st.shop["name"],
                            "seller_base_region": "ID", "user_type": 0, "granted_scopes": ["seller.order.info", "seller.finance.info",
                                                                                            "seller.product.basic"]})

        # -- the signed API
        def api(self, method: str, path: str, q: dict, body: dict):
            if method == "GET" and path == "/authorization/202309/shops":
                return self.ok({"shops": st.shops})
            if method == "POST" and path == "/order/202309/orders/search":
                def within(o, f):
                    v = o.get(f) or 0
                    return (f + "_ge" not in body or v >= int(body[f + "_ge"])) and (f + "_lt" not in body or v < int(body[f + "_lt"]))
                with st.lock:
                    rows = [o for o in st.orders.values() if within(o, "update_time") and within(o, "create_time")
                            and (not body.get("order_status") or o["status"] == body["order_status"])]
                    key = "update_time" if q.get("sort_field") == "update_time" else "create_time"
                    rows.sort(key=lambda o: (o.get(key) or 0, o["id"]), reverse=q.get("sort_order", "DESC") != "ASC")
                    chunk, nxt = page_of(copy.deepcopy(rows), q, st.max_page_size)
                if chunk is None:
                    return self.err(400, 36009004, "Invalid page_token")
                return self.ok({"total_count": len(rows), "next_page_token": nxt, "orders": chunk})
            if method == "GET" and path == "/order/202507/orders":
                ids = [i for i in (q.get("ids") or "").split(",") if i]
                if not ids or len(ids) > 50:
                    return self.err(400, 36009004, "ids: 1 to 50 order ids")
                with st.lock:
                    if any(i not in st.orders for i in ids):
                        return self.err(400, 21008111, "The order or package does not belong to the current seller")
                    return self.ok({"orders": [copy.deepcopy(st.orders[i]) for i in ids]})
            if method == "GET" and path == "/finance/202309/statements":
                if q.get("sort_field") != "statement_time":
                    return self.err(400, 36009004, "sort_field must be statement_time")
                with st.lock:
                    rows = [s for s in st.statements.values()
                            if (not q.get("statement_time_ge") or s["statement_time"] >= int(q["statement_time_ge"]))
                            and (not q.get("statement_time_lt") or s["statement_time"] < int(q["statement_time_lt"]))
                            and (not q.get("payment_status") or s["payment_status"] == q["payment_status"])]
                    rows.sort(key=lambda s: (s["statement_time"], s["id"]), reverse=q.get("sort_order", "ASC") == "DESC")
                    chunk, nxt = page_of(copy.deepcopy(rows), q, st.max_page_size)
                if chunk is None:
                    return self.err(400, 36009004, "Invalid page_token")
                return self.ok({"next_page_token": nxt, "statements": chunk})
            m = re.fullmatch(r"/finance/202501/statements/([^/]+)/statement_transactions", path)
            if method == "GET" and m:
                with st.lock:
                    s = st.statements.get(m.group(1))
                    if not s:
                        return self.err(404, 36009004, "Statement not found")
                    txs = copy.deepcopy(st.transactions.get(s["id"], []))
                chunk, nxt = page_of(txs, q, st.max_page_size)
                if chunk is None:
                    return self.err(400, 36009004, "Invalid page_token")
                return self.ok({"id": s["id"], "create_time": s["statement_time"], "status": "SETTLED", "currency": s["currency"],
                                "payable_amount": s["settlement_amount"], "total_reserve_amount": "0",
                                "total_settlement_amount": s["settlement_amount"], "total_count": len(txs),
                                "next_page_token": nxt, "transactions": chunk})
            m = re.fullmatch(r"/product/202309/products/([^/]+)/inventory/update", path)
            if method == "POST" and m:
                return self.inventory(m.group(1), body)
            if method == "POST" and path == "/product/202502/products/search":
                want = set(body.get("seller_skus") or [])
                status = body.get("status") or "ALL"
                with st.lock:
                    rows = [p for p in st.products.values() if (status == "ALL" or p["status"] == status)
                            and (not want or any(s["seller_sku"] in want for s in p["skus"]))]
                    chunk, nxt = page_of(copy.deepcopy(rows), q, st.max_page_size)
                if chunk is None:
                    return self.err(400, 36009004, "Invalid page_token")
                return self.ok({"total_count": len(rows), "next_page_token": nxt, "products": chunk})
            m = re.fullmatch(r"/fulfillment/202309/packages/([^/]+)", path)
            if method == "GET" and m:
                with st.lock:
                    p = st.packages.get(m.group(1))
                    if not p:
                        return self.err(400, 21008111, "The order or package does not belong to the current seller")
                    return self.ok(copy.deepcopy(p))
            return self.err(404, 36009009, "Invalid path")

        def inventory(self, product_id: str, body: dict):
            with st.lock:
                p = st.products.get(product_id)
                if not p:
                    return self.err(400, 12052990, "Batch update inventory error: product not found")
                errors = []
                for s in body.get("skus") or []:
                    sku = next((x for x in p["skus"] if x["id"] == str(s.get("id"))), None)
                    if not sku:
                        errors.append({"code": 12052990, "message": "SKU not found", "detail": {"sku_id": str(s.get("id"))}})
                        continue
                    for inv in s.get("inventory") or []:
                        qty = inv.get("quantity")
                        if isinstance(qty, str) and qty.isdigit():
                            qty = int(qty)   # the kernel templates numbers as text today; TikTok documents an integer
                        if not isinstance(qty, int) or qty < 0:
                            errors.append({"code": 12052990, "message": "Invalid quantity", "detail": {"sku_id": sku["id"]}})
                            continue
                        wh = str(inv.get("warehouse_id") or "") or sku["inventory"][0]["warehouse_id"]
                        row = next((x for x in sku["inventory"] if x["warehouse_id"] == wh), None)
                        if not row:
                            errors.append({"code": 12052097, "message": "The warehouse does not exist",
                                           "detail": {"sku_id": sku["id"], "extra_errors": [{"warehouse_id": wh, "code": 12052097}]}})
                            continue
                        row["quantity"] = qty   # absolute, never a delta
                p["update_time"] = int(st.clock())
            return self.ok({"errors": errors} if errors else {})

        # -- /simulate/*
        def simulate(self, path: str, raw: bytes):
            parts = path.strip("/").split("/")
            act = parts[1] if len(parts) > 1 else ""
            now = int(st.clock())
            if act == "expire_token":
                with st.lock:
                    for k in st.access:
                        st.access[k] = now - 1
                return self.send(200, {"expired": len(st.access)})
            if act == "authorize":
                code = "mock-auth-" + secrets.token_hex(8)
                with st.lock:
                    st.auth_codes[code] = now + 1800   # 30 minutes, single use
                return self.send(200, {"auth_code": code})
            if act == "create":
                try:
                    req = json.loads(raw or b"{}")
                    skus = list(req["skus"])
                    platform = req.get("commerce_platform", "TIKTOK_SHOP")
                except (ValueError, KeyError, TypeError):
                    return self.send(400, {"error": "body {\"commerce_platform\", \"skus\": [seller_sku, ...]}"})
                if platform not in ("TIKTOK_SHOP", "TOKOPEDIA", "TIKTOK_SHOP_APP"):
                    return self.send(400, {"error": "commerce_platform: TIKTOK_SHOP, TOKOPEDIA or TIKTOK_SHOP_APP"})
                with st.lock:
                    o = self.new_order(platform, skus, now)
                    snap = copy.deepcopy(o)
                if o is None:
                    return self.send(400, {"error": "unknown seller_sku"})
                return self.send(200, {"order": snap, "notified": st.webhook(snap)})
            if len(parts) != 3:
                return self.send(404, {"error": "not found"})
            with st.lock:
                o = st.orders.get(parts[2])
                if not o:
                    return self.send(404, {"error": "order not found"})
                cur = o["status"]
                if act == "pay" and cur in ("UNPAID", "ON_HOLD"):
                    o.setdefault("paid_time", now)
                    st.set_status(o, "AWAITING_SHIPMENT")
                elif act == "hold" and cur in ("UNPAID", "AWAITING_SHIPMENT"):
                    o.setdefault("paid_time", now)
                    st.set_status(o, "ON_HOLD", "AWAITING_SHIPMENT")
                elif act == "ship" and cur in ("AWAITING_SHIPMENT", "AWAITING_COLLECTION"):
                    tracking = "JX" + o["id"][-10:]
                    o["tracking_number"], o["rts_time"] = tracking, now
                    for li in o["line_items"]:
                        li["tracking_number"] = tracking
                    st.set_status(o, "IN_TRANSIT", package_status="FULFILLING")
                    pkg = o["packages"][0]["id"]
                    st.packages[pkg] = {"package_id": pkg, "orders": [{"id": o["id"]}], "package_status": "FULFILLING",
                                        "shipping_provider_name": o.get("shipping_provider", "J&T Express"),
                                        "shipping_type": "TIKTOK", "tracking_number": tracking, "handover_method": "PICKUP",
                                        "order_line_item_ids": [li["id"] for li in o["line_items"]], "create_time": now,
                                        "update_time": now}
                elif act == "deliver" and cur == "IN_TRANSIT":
                    o["delivery_time"] = now
                    st.set_status(o, "DELIVERED")
                elif act == "complete" and cur == "DELIVERED":
                    st.set_status(o, "COMPLETED", package_status="COMPLETED")
                    st.settle(o)
                elif act == "cancel" and cur not in ("IN_TRANSIT", "DELIVERED", "COMPLETED", "CANCELLED"):
                    o["cancel_time"], o["cancellation_initiator"], o["cancel_reason"] = now, "BUYER", "Pembeli membatalkan pesanan"
                    st.set_status(o, "CANCELLED", package_status="CANCELLED")
                elif act in ("pay", "hold", "ship", "deliver", "complete", "cancel"):
                    return self.send(409, {"error": f"cannot {act} an order in {cur}"})
                else:
                    return self.send(404, {"error": "not found"})
                snap = copy.deepcopy(o)
            # the push is sent outside the lock: the receiver may call the mock back
            return self.send(200, {"order": snap, "notified": st.webhook(snap)})

        def new_order(self, platform: str, skus: list, now: int):
            by_sku = {s["seller_sku"]: (p, s) for p in st.products.values() for s in p["skus"]}
            if not skus or any(x not in by_sku for x in skus):
                return None
            oid = st.new_id("5770", 18)
            pkg = st.new_id("1152")
            lines = []
            for n, x in enumerate(skus):
                p, s = by_sku[x]
                lines.append({"id": f"{oid}{n + 1:02d}", "product_id": p["id"], "product_name": p["title"], "sku_id": s["id"],
                              "sku_name": "", "seller_sku": x, "original_price": s["price"]["sale_price"],
                              "sale_price": s["price"]["sale_price"], "platform_discount": "0", "seller_discount": "0",
                              "currency": "IDR", "display_status": "UNPAID", "package_id": pkg, "package_status": "TO_FULFILL",
                              "tracking_number": "", "is_gift": False})
            total = sum(int(li["sale_price"]) for li in lines)
            o = {"id": oid, "status": "UNPAID", "commerce_platform": platform, "order_type": "NORMAL", "create_time": now,
                 "update_time": now, "is_on_hold_order": False, "is_cod": False, "shipping_type": "TIKTOK",
                 "fulfillment_type": "FULFILLMENT_BY_SELLER", "shipping_provider": "J&T Express", "tracking_number": "",
                 "packages": [{"id": pkg}], "line_items": lines,
                 "payment": {"currency": "IDR", "original_total_product_price": str(total), "seller_discount": "0",
                             "platform_discount": "0", "sub_total": str(total), "shipping_fee": "10000",
                             "total_amount": str(total + 10000)}}
            st.orders[oid] = o
            return o

    return H


# ------------------------------------------------------------------------------------------------------------ poll
class TikTokError(Exception):
    def __init__(self, code: int, message: str):
        super().__init__(f"TikTok Shop code {code}: {message}")
        self.code = code


def check_api(url: str, flag: str = "--api") -> str:
    """A base URL the relay may send the app secret and tokens to: https to a host of connector.rk, or plain http to
    this machine (the mock)."""
    u = urllib.parse.urlsplit(url or "")
    if (u.scheme == "https" and u.hostname in HOSTS) or (u.scheme == "http" and u.hostname in LOCAL):
        return url.rstrip("/")
    raise SystemExit(f"{flag} must be https://{HOSTS[0]}, https://{HOSTS[1]} or http://127.0.0.1:<port> (the mock)")


def check_rangka(rangka: str) -> str:
    """The kernel URL the relay may send RANGKA_TOKEN to: https, or plain http to this machine."""
    u = urllib.parse.urlsplit(rangka or "")
    if (u.scheme == "https" and u.hostname) or (u.scheme == "http" and u.hostname in LOCAL):
        return rangka.rstrip("/")
    raise SystemExit("--rangka must be https://<kernel> or http://127.0.0.1:<port>: RANGKA_TOKEN never goes over plain http to another machine")


class Client:
    """Signs, sends, refreshes once on 105002 and sends the call again. Never logs a secret or a token."""

    def __init__(self, api: str, auth_api: str, creds: dict, token_file: str | None = None):
        self.api, self.auth_api = check_api(api, "--api"), check_api(auth_api, "--auth-api")
        self.c = dict(creds)
        self.token_file = token_file
        self.refreshed = False
        self.calls = 0

    def call(self, method: str, path: str, query: dict | None = None, body: dict | None = None, shop: bool = True) -> dict:
        for attempt in (1, 2):
            q = {"app_key": self.c["app_key"], **(query or {})}
            if shop:
                q["shop_cipher"] = self.c["shop_cipher"]
            q = {k: str(v) for k, v in q.items() if v is not None and v != ""}
            q["timestamp"] = str(int(time.time()))
            raw = json.dumps(body, separators=(",", ":")).encode() if body is not None else b""
            q["sign"] = sign_request(self.c["app_secret"], path, q, raw)
            req = urllib.request.Request(f"{self.api}{path}?{urllib.parse.urlencode(q)}", data=raw if body is not None else None,
                                         method=method, headers={"x-tts-access-token": self.c["access_token"],
                                                                 "Content-Type": "application/json"})
            self.calls += 1
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    out = json.loads(r.read())
            except urllib.error.HTTPError as e:
                try:
                    out = json.loads(e.read())
                except ValueError:
                    raise TikTokError(e.code, "HTTP error without a TikTok envelope") from None
            if out.get("code") == 0:
                return out.get("data") or {}
            if out.get("code") == 105002 and attempt == 1 and not self.refreshed:
                self.refresh()
                continue
            raise TikTokError(out.get("code"), out.get("message", ""))
        raise TikTokError(105002, "access token still expired after a refresh")

    def refresh(self):
        if not self.token_file:
            # TikTok Shop may rotate the pair on a refresh: without a file to keep the new pair it would be lost
            raise SystemExit("the access token expired: rerun with --token-file so the refreshed token pair is kept")
        q = urllib.parse.urlencode({"app_key": self.c["app_key"], "app_secret": self.c["app_secret"],
                                    "refresh_token": self.c["refresh_token"], "grant_type": "refresh_token"})
        try:
            with urllib.request.urlopen(f"{self.auth_api}/api/v2/token/refresh?{q}", timeout=30) as r:
                out = json.loads(r.read())
        except urllib.error.HTTPError as e:
            out = json.loads(e.read() or b"{}")
        if out.get("code") != 0:
            raise TikTokError(out.get("code"), "token refresh refused: " + str(out.get("message", "")))
        d = out["data"]
        self.c["access_token"], self.c["refresh_token"] = d["access_token"], d["refresh_token"]
        self.refreshed = True
        # the rotated refresh token is stored before the new access token is used
        if self.token_file:
            save_tokens(self.token_file, {"access_token": d["access_token"], "refresh_token": d["refresh_token"],
                                          "access_token_expire_in": d.get("access_token_expire_in"),
                                          "refresh_token_expire_in": d.get("refresh_token_expire_in")})


def save_tokens(path: str, tokens: dict):
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(tokens, f)
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


def pages(client: Client, method: str, path: str, query: dict, body: dict | None, key: str, max_pages: int = 50):
    """Every row of every page, following next_page_token until it is empty (or seen before, or max_pages)."""
    token, seen = "", set()
    for _ in range(max_pages):
        data = client.call(method, path, dict(query, page_token=token), body)
        yield data, data.get(key) or []
        token = data.get("next_page_token") or ""
        if not token or token in seen:
            return
        seen.add(token)


def collect(client: Client, since_days: int = 2, settled_days: int = 7, page_size: int = 50, now: int | None = None) -> list:
    """One poll of both triggers; returns [(flow, event), ...] in the order the kernel would see them."""
    now = int(now or time.time())
    midnight = now - now % 86400   # the spec's (days_between('1970-01-01', today()) - n) * 86400
    out = []
    for _, orders in pages(client, "POST", "/order/202309/orders/search",
                           {"page_size": page_size, "sort_field": "update_time", "sort_order": "ASC"},
                           {"update_time_ge": midnight - since_days * 86400}, "orders"):
        out += [(FLOW_STAGED, order_event(o)) for o in orders]
    statements = []
    for _, rows in pages(client, "GET", "/finance/202309/statements", {"sort_field": "statement_time", "page_size": 100,
                                                                       "statement_time_ge": midnight - settled_days * 86400,
                                                                       "payment_status": "PAID"}, None, "statements"):
        statements += [s for s in rows if s["id"] not in {x["id"] for x in statements}]
    for s in statements:
        for _, txs in pages(client, "GET", f"/finance/202501/statements/{s['id']}/statement_transactions",
                            {"sort_field": "order_create_time", "page_size": page_size}, None, "transactions"):
            out += [(FLOW_SETTLED, settled_event(s, t)) for t in txs]
    return out


def deliver(rangka: str, rangka_token: str, events: list) -> dict:
    res = {"events": len(events), "delivered": 0, "refused": []}
    for flow, ev in events:
        req = urllib.request.Request(f"{rangka.rstrip('/')}/api/v1/flows/{flow}/run", data=json.dumps({"event": ev}).encode(),
                                     method="POST", headers={"Content-Type": "application/json",
                                                             "Authorization": f"Bearer {rangka_token}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                res["delivered"] += r.status == 200
        except urllib.error.HTTPError as e:
            res["refused"].append({"flow": flow, "order_id": ev.get("order_id"), "status": e.code,
                                   "body": e.read()[:300].decode(errors="replace")})
    return res


def creds_from_env(token_file: str | None) -> dict:
    c = {"app_key": os.environ.get("TIKTOK_SHOP_APP_KEY", ""), "app_secret": os.environ.get("TIKTOK_SHOP_APP_SECRET", ""),
         "shop_cipher": os.environ.get("TIKTOK_SHOP_SHOP_CIPHER", ""),
         "access_token": os.environ.get("TIKTOK_SHOP_ACCESS_TOKEN", ""), "refresh_token": os.environ.get("TIKTOK_SHOP_REFRESH_TOKEN", "")}
    if token_file and os.path.exists(token_file):
        saved = json.load(open(token_file))
        c["access_token"] = saved.get("access_token") or c["access_token"]
        c["refresh_token"] = saved.get("refresh_token") or c["refresh_token"]
    return c


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=8931)
    s.add_argument("--app-key", default="demo-app-key")
    s.add_argument("--fixtures", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "fixtures"))
    s.add_argument("--webhook-url", default=None, help="where /simulate/* sends webhook type 1 (the kernel hook)")
    s.add_argument("--max-page-size", type=int, default=100, help="cap page_size so paging is exercised (e.g. 2)")
    s.add_argument("--keep-times", action="store_true", help="serve fixture times unchanged")
    p = sub.add_parser("poll")
    p.add_argument("--api", required=True, help="https://open-api.tiktokglobalshop.com or the mock")
    p.add_argument("--auth-api", default="https://auth.tiktok-shops.com", help="token host (the mock serves both)")
    p.add_argument("--rangka", help="kernel base URL (not needed with --dry-run)")
    p.add_argument("--dry-run", action="store_true", help="print the events as JSON lines, post nothing")
    p.add_argument("--since-days", type=int, default=2)
    p.add_argument("--settled-days", type=int, default=7)
    p.add_argument("--page-size", type=int, default=50)
    p.add_argument("--token-file", help="JSON file (0600) holding the rotated tokens; read first when it exists; "
                   "needed unless --dry-run")
    a = ap.parse_args(argv)
    if a.cmd == "serve":
        secret = os.environ.get("TIKTOK_SHOP_MOCK_APP_SECRET", "")
        if not dummy_key(secret):
            ap.error("set TIKTOK_SHOP_MOCK_APP_SECRET to a dummy secret (a part dummy or test, e.g. tts-test-dummy-secret); "
                     "never a real app secret")
        st = State(a.fixtures, a.app_key, secret, os.environ.get("TIKTOK_SHOP_MOCK_ACCESS_TOKEN", "demo-tts-access-token"),
                   os.environ.get("TIKTOK_SHOP_MOCK_REFRESH_TOKEN", "demo-tts-refresh-token"), a.webhook_url, a.keep_times,
                   a.max_page_size)
        srv = ThreadingHTTPServer(("127.0.0.1", a.port), handler(st))
        print(f"tiktok_shop mock: http://127.0.0.1:{srv.server_address[1]}"
              + (f" (webhooks to {a.webhook_url})" if a.webhook_url else ""), flush=True)
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            pass
        return 0
    # hosts first: nothing is sent anywhere before every address is allowed
    check_api(a.api, "--api")
    check_api(a.auth_api, "--auth-api")
    if not a.dry_run:
        if a.rangka:
            check_rangka(a.rangka)
        if not a.token_file:
            ap.error("--token-file is needed unless --dry-run: TikTok Shop rotates the token pair on a refresh")
    creds = creds_from_env(a.token_file)
    missing = [k for k in ("app_key", "app_secret", "shop_cipher", "access_token") if not creds[k]]
    if missing:
        sys.exit("set " + ", ".join("TIKTOK_SHOP_" + k.upper() for k in missing))
    rangka_token = os.environ.get("RANGKA_TOKEN", "")
    if not a.dry_run and (not a.rangka or not rangka_token):
        sys.exit("set --rangka and RANGKA_TOKEN (an API token of the tenant's owner or head office), or use --dry-run")
    client = Client(a.api, a.auth_api, creds, a.token_file)
    try:
        events = collect(client, a.since_days, a.settled_days, a.page_size)
    except TikTokError as e:
        sys.exit(str(e))
    if a.dry_run:
        for flow, ev in events:
            print(json.dumps({"flow": flow, "event": ev}, ensure_ascii=False))
        print(json.dumps({"events": len(events), "calls": client.calls, "refreshed": client.refreshed}), file=sys.stderr)
        return 0
    res = deliver(a.rangka, rangka_token, events)
    res.update(calls=client.calls, refreshed=client.refreshed)
    print(json.dumps(res))
    return 1 if res["refused"] else 0


if __name__ == "__main__":
    sys.exit(main())
