#!/usr/bin/env python3
"""A local stand-in for the Shopee Open Platform API (v2), and a poller that does what the kernel's poll triggers of
connector `shopee` will do once the kernel signs query strings (Juragan proposals/marketplace-connector-runtime.md).
For demos and tests without a Shopee developer account.

    SHOPEE_MOCK_PARTNER_KEY=test-dummy-shopee-key python3 shopee/mock/shopee_mock.py serve --port 8931 \
        [--partner-id 2009001] [--shop-id 880001] [--day 2026-11-19] [--max-page-size 2] \
        [--push-url http://127.0.0.1:8080/hooks/<org>/shopee/push_received]

    SHOPEE_PARTNER_ID=2009001 SHOPEE_PARTNER_KEY=test-dummy-shopee-key SHOPEE_SHOP_ID=880001 \
    SHOPEE_ACCESS_TOKEN=... SHOPEE_REFRESH_TOKEN=... RANGKA_TOKEN=... \
        python3 shopee/mock/shopee_mock.py poll --api http://127.0.0.1:8931 --rangka http://127.0.0.1:8101 \
        --token-file ~/.shopee-tokens.json [--since-days 2] [--page-size 100] [--dry-run]

Dummy keys only for serve. The mock signs any push it is asked for (/simulate/*, no authentication): it is a signing
oracle for its key. So it listens on 127.0.0.1 only, reads the partner key from SHOPEE_MOCK_PARTNER_KEY (not argv,
which `ps` shows), and refuses a key that does not say it is a dummy (a part "dummy" or "test" between - _ or .). Its
first token pair comes from SHOPEE_MOCK_ACCESS_TOKEN and SHOPEE_MOCK_REFRESH_TOKEN (dummy defaults when unset). It
never prints a key or a token.

serve answers in Shopee's formats, from the fixtures folder (get_order_detail.json, get_settlement.json,
list_settlements.json, list_items.json, get_models.json) and in-memory state. Every /api/v2 call is checked as Shopee
documents it (developer-guide/16 and 20): partner_id, a timestamp within 5 minutes, sign = lower-case hex
HMAC-SHA256(partner key, partner_id + path + timestamp [+ access_token + shop_id]), for a shop call an access token that
is known and not expired and the shop's shop_id. Errors use Shopee's envelope {"error", "message", "request_id"}:
error_param (400), error_sign, error_auth, invalid_acceess_token (403; Shopee's spelling), business errors (400).
- POST /api/v2/auth/token/get            code (from /simulate/code) -> a token pair (public sign)
- POST /api/v2/auth/access_token/get     refresh: a new pair; the refresh token rotates, the old one replays the same
                                          new pair for 4 hours unless the new one was used, the previous access token
                                          stays valid 5 minutes
- GET  /api/v2/order/get_order_list      window <= 15 days by create_time or update_time, page_size 1-100 (capped by
                                          --max-page-size), cursor paging (more, next_cursor)
- GET  /api/v2/order/get_order_detail    1-50 order_sn; only the requested optional fields (buyer fields come back
                                          masked when asked for, as Shopee masks them; the poller never asks)
- GET  /api/v2/payment/get_escrow_list, get_escrow_detail     released escrows and their order_income
- POST /api/v2/product/update_stock      sets seller stock (absolute); refuses a stock below the reserved stock
- GET  /api/v2/product/get_item_list, get_model_list; GET /api/v2/logistics/get_tracking_number
- GET  /calls                            every request (access tokens, refresh tokens and codes masked), for tests
- POST /simulate/code                    a one-time authorization code (10 minutes), as the redirect would bring
- POST /simulate/create [body {"lines": [{"item_id", "model_id", "qty"}]}], /simulate/pay/<sn>, /simulate/ship/<sn>,
  /simulate/complete/<sn> (also releases its escrow), /simulate/cancel/<sn>[?status=IN_CANCEL]: each bumps the
  update time and sends push code 3 (and code 4 on ship, first) to --push-url, signed as Shopee signs: Authorization =
  hex HMAC-SHA256(partner key, <push url> + '|' + <raw body>).
--day D moves every fixture time by whole days so that the fixtures' last update day becomes D, then one day further
back while the newest fixture time would still lie after now (so --day <today> before 08:30 WIB lands on yesterday and
a poll finds every fixture): one fixture set serves any demo date.

poll is the relay that stands in for the kernel (marketplace/README.md, "Until the kernel signs these requests"): it
signs every request, refreshes once on an expired-token error and retries, lists the orders changed in the last
--since-days (windows of 15 days, every page), reads their details in batches of 50, lists the escrows released in the
same days and reads each one, builds every event with exactly the emit mapping of order_changed and order_settled in
shopee/connector.rk, and posts it to the tenant's flow shopee_order_staged or shopee_order_settled
(POST /api/v1/flows/<name>/run, {"event": ...}, Bearer RANGKA_TOKEN), as moka_mock.py poll does. --dry-run prints the
events as JSON lines instead and posts nothing. --token-file (0600) keeps the rotated pair: it is read first when it
exists and rewritten after every refresh, dry runs included (Shopee has already rotated the pair, and the old refresh
token only replays it for 4 hours). A run that posts refuses to start without --token-file. --api is the mock, or once
keys exist Shopee's sandbox or live host: the partner key and tokens go only over https to the hosts of connector.rk,
or plain http to 127.0.0.1 or localhost. RANGKA_TOKEN goes only over https, or plain http to 127.0.0.1 or localhost.
Both are checked before any request is sent.

Each poll reads a fixed window and keeps no cursor: after an outage longer than the window (relay off, kernel down,
token expired) run poll --since-days N once, N covering the outage. The flows' idempotency keys (marketplace/README.md,
"Flow keys") make the overlap and the repeated events of every poll harmless.

Synthetic data only. Stdlib only.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import hmac
import json
import os
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WIB = timezone(timedelta(hours=7))
HOSTS = ("partner.shopeemobile.com", "openplatform.sandbox.test-stable.shopee.sg")
LOCAL = ("127.0.0.1", "localhost", "::1")
PUBLIC_PATHS = ("/api/v2/auth/token/get", "/api/v2/auth/access_token/get")
# what get_order_detail asks for: the same list as action get_order_detail in connector.rk; no buyer field
OPTIONAL_FIELDS = ("item_list,pay_time,total_amount,package_list,shipping_carrier,payment_method,cancel_by,cancel_reason,"
                   "buyer_cancel_reason,pickup_done_time,fulfillment_flag")
DEFAULT_FIELDS = ("order_sn", "region", "currency", "cod", "order_status", "message_to_seller", "create_time", "update_time",
                  "days_to_ship", "ship_by_date", "booking_sn")
BUYER_FIELDS = ("recipient_address", "buyer_username", "buyer_user_id")
# the flag table of marketplace/README.md, column shopee
SOLD = ("READY_TO_SHIP", "PROCESSED", "RETRY_SHIP", "SHIPPED", "TO_CONFIRM_RECEIVE", "COMPLETED")
SHIPPED = ("SHIPPED", "TO_CONFIRM_RECEIVE", "COMPLETED")
TOKEN_ERRORS = ("error_auth", "invalid_acceess_token", "invalid_access_token")
ACCESS_TTL, REFRESH_TTL, REPLAY, OLD_ACCESS_GRACE, CODE_TTL, TS_WINDOW = 14400, 30 * 86400, 4 * 3600, 300, 600, 300
FLOW_STAGED, FLOW_SETTLED = "shopee_order_staged", "shopee_order_settled"


# ---- signing (pure, used by the tests) ------------------------------------------------------------------------------

def base_shop(partner_id, path: str, timestamp, access_token: str, shop_id) -> str:
    """Shopee's base string of a shop call: partner_id, path, timestamp, access_token, shop_id, no separator."""
    return f"{partner_id}{path}{timestamp}{access_token}{shop_id}"


def base_public(partner_id, path: str, timestamp) -> str:
    """Shopee's base string of a public call (token calls): partner_id, path, timestamp."""
    return f"{partner_id}{path}{timestamp}"


def _hmac_hex(key: str, msg: bytes) -> str:
    return hmac.new(key.encode(), msg, hashlib.sha256).hexdigest()


def sign_shop(partner_key: str, partner_id, path: str, timestamp, access_token: str, shop_id) -> str:
    return _hmac_hex(partner_key, base_shop(partner_id, path, timestamp, access_token, shop_id).encode())


def sign_public(partner_key: str, partner_id, path: str, timestamp) -> str:
    return _hmac_hex(partner_key, base_public(partner_id, path, timestamp).encode())


def sign_push(partner_key: str, url: str, body: bytes) -> str:
    """The Authorization header of a push: hex HMAC-SHA256(partner key, url + '|' + raw body)."""
    return _hmac_hex(partner_key, url.encode() + b"|" + body)


def dummy_key(secret: str) -> bool:
    """True for a key that says it is a dummy: the mock signs whatever it is asked, so a real key must never reach it."""
    return bool(re.search(r"(^|[-_.])(dummy|test)([-_.]|$)", secret or "", re.IGNORECASE))


# ---- the emit mappings of shopee/connector.rk -----------------------------------------------------------------------

def order_event(o: dict) -> dict:
    """Trigger order_changed, item = one order of get_order_detail (a field Shopee did not send is null)."""
    st = o.get("order_status")
    return {
        "channel": "shopee", "storefront": "SHOPEE", "order_id": o.get("order_sn"), "status": st,
        "sold": st in SOLD, "shipped": st in SHIPPED, "completed": st == "COMPLETED", "cancelled": st == "CANCELLED",
        "created_at": o.get("create_time"), "paid_at": o.get("pay_time"), "updated_at": o.get("update_time"),
        "currency": o.get("currency"), "buyer_total": o.get("total_amount"), "lines": o.get("item_list"),
    }


def settled_event(escrow: dict, settlement: dict) -> dict:
    """Trigger order_settled, escrow = the get_escrow_list item, item = the get_escrow_detail response."""
    inc = settlement.get("order_income") or {}
    return {
        "channel": "shopee", "order_id": settlement.get("order_sn"), "currency": "IDR",
        "settled_at": escrow.get("escrow_release_time"), "net": inc.get("escrow_amount"),
        "adjusted_net": inc.get("escrow_amount_after_adjustment"),
        "commission_fee": inc.get("commission_fee"), "service_fee": inc.get("service_fee"),
        "transaction_fee": inc.get("seller_transaction_fee"), "withheld_tax": inc.get("withholding_tax"), "income": inc,
    }


# ---- the mock -------------------------------------------------------------------------------------------------------

class ShopeeError(Exception):
    def __init__(self, status: int, error: str, message: str):
        super().__init__(f"{error}: {message}")
        self.status, self.error, self.message = status, error, message


def _rid() -> str:
    return uuid.uuid4().hex


def _token() -> str:
    return uuid.uuid4().hex  # Shopee's tokens are 32 hex characters


def tracking_for(sn: str) -> str:
    return f"SPXID0{sn[:6]}{sn[-4:]}"


def _wib_day(ts: int) -> date:
    return datetime.fromtimestamp(ts, WIB).date()


class Mock:
    """The mock's memory. partner_key is a dummy key: whoever reaches the mock can have anything signed with it."""

    def __init__(self, fixtures: str, partner_id: int, partner_key: str, shop_id: int, access_token: str,
                 refresh_token: str, day: str | None = None, max_page_size: int = 100, push_url: str | None = None,
                 clock=None):
        self.partner_id, self.partner_key, self.shop_id = int(partner_id), partner_key, int(shop_id)
        self.max_page_size, self.push_url = max(1, int(max_page_size)), push_url
        self.clock = clock or time.time  # tests pass their own clock
        self.lock = threading.RLock()
        self.calls: list[dict] = []
        self.pushes: list[dict] = []
        ld = lambda n: json.load(open(os.path.join(fixtures, n)))  # noqa: E731
        orders = ld("get_order_detail.json")["response"]["order_list"]
        settlement = ld("get_settlement.json")["response"]
        escrows = ld("list_settlements.json")["response"]["escrow_list"]
        self.items = ld("list_items.json")["response"]["item"]
        models = ld("get_models.json")["response"]
        shift = 0
        if day:
            last = max(_wib_day(o["update_time"]) for o in orders)
            shift = (date.fromisoformat(day) - last).days * 86400
            # never in the future: a poll's window ends at now, so a fixture after now would be missed
            newest = max([o.get(k) or 0 for o in orders for k in ("create_time", "update_time", "pay_time", "pickup_done_time")]
                         + [e["escrow_release_time"] for e in escrows]) + shift
            if newest > self.now():
                shift -= -(-(newest - self.now()) // 86400) * 86400
        self.orders: dict[str, dict] = {}
        for o in orders:
            o = copy.deepcopy(o)
            for k in ("create_time", "update_time", "pay_time", "pickup_done_time", "ship_by_date"):
                if o.get(k):
                    o[k] += shift
            if o["order_status"] in SHIPPED:
                o["_tracking"] = tracking_for(o["order_sn"])
            self.orders[o["order_sn"]] = o
        self.settlements: dict[str, dict] = {}
        for e in escrows:
            e = dict(e, escrow_release_time=e["escrow_release_time"] + shift)
            if e["order_sn"] == settlement["order_sn"]:
                self.settlements[e["order_sn"]] = {"escrow": e, "detail": copy.deepcopy(settlement)}
        # catalogue: a price per (item_id, model_id) from the order lines, stock from get_models.json (0 otherwise)
        self.catalogue: dict[tuple, dict] = {}
        for o in orders:
            for li in o.get("item_list") or []:
                self.catalogue.setdefault((li["item_id"], li["model_id"]), {k: li[k] for k in (
                    "item_id", "item_name", "item_sku", "model_id", "model_name", "model_sku", "model_original_price")})
        self.models = {3001002: models}
        self.stock = {k: {"stock": 0, "reserved": 0} for k in self.catalogue}
        for m in models["model"]:
            si = m["stock_info_v2"]
            self.stock[(3001002, m["model_id"])] = {"stock": si["seller_stock"][0]["stock"],
                                                   "reserved": si["summary_info"]["total_reserved_stock"]}
        now = self.now()
        self.access = {access_token: now + ACCESS_TTL}           # access token -> expires at
        self.refresh = {refresh_token: {"expires": now + REFRESH_TTL, "used_at": None, "next": None}}
        self.codes: dict[str, int] = {}
        self.seq = 0

    def now(self) -> int:
        return int(self.clock())

    # -- checks -----------------------------------------------------------------------------------------------------

    def check(self, path: str, q: dict):
        """Common parameters and the signature, as Shopee checks them (developer-guide/16)."""
        for p in ("partner_id", "timestamp", "sign"):
            if not q.get(p):
                raise ShopeeError(400, "error_param", {"partner_id": "There is no partner_id in query.", "timestamp": "no timestamp",
                                                       "sign": "There is no sign in query."}[p])
        if q["partner_id"] != str(self.partner_id):
            raise ShopeeError(403, "error_auth", "partner_id is invalid")
        try:
            ts = int(q["timestamp"])
        except ValueError:
            raise ShopeeError(400, "error_param", "Invalid timestamp")
        if abs(self.now() - ts) > TS_WINDOW:
            raise ShopeeError(400, "error_param", "Timestamp is expired.")
        if path in PUBLIC_PATHS:
            want = sign_public(self.partner_key, q["partner_id"], path, q["timestamp"])
        else:
            for p, msg in (("access_token", "There is no access_token in query."), ("shop_id", "There is no shop_id in query.")):
                if not q.get(p):
                    raise ShopeeError(400, "error_param", msg)
            want = sign_shop(self.partner_key, q["partner_id"], path, q["timestamp"], q["access_token"], q["shop_id"])
        if not hmac.compare_digest(q["sign"], want):
            raise ShopeeError(403, "error_sign", "Wrong sign.")
        if path in PUBLIC_PATHS:
            return
        if q["shop_id"] != str(self.shop_id):
            raise ShopeeError(403, "error_auth", "Invalid partner_id or shopid.")
        exp = self.access.get(q["access_token"])
        if exp is None:
            raise ShopeeError(403, "error_auth", "Invalid access_token.")
        if self.now() >= exp:
            raise ShopeeError(403, "invalid_acceess_token", "Invalid access_token.")

    # -- tokens ------------------------------------------------------------------------------------------------------

    def _issue(self) -> dict:
        at, rt = _token(), _token()
        now = self.now()
        self.access[at] = now + ACCESS_TTL
        self.refresh[rt] = {"expires": now + REFRESH_TTL, "used_at": None, "next": None}
        return {"access_token": at, "refresh_token": rt, "expire_in": ACCESS_TTL}

    def token_get(self, body: dict) -> dict:
        code = str(body.get("code") or "")
        exp = self.codes.pop(code, None)
        if exp is None or self.now() >= exp:
            raise ShopeeError(403, "error_auth", "Invalid code.")
        if str(body.get("shop_id")) != str(self.shop_id) or str(body.get("partner_id")) != str(self.partner_id):
            raise ShopeeError(403, "error_auth", "Invalid partner_id or shopid.")
        return self._issue()

    def token_refresh(self, body: dict) -> dict:
        if str(body.get("shop_id")) != str(self.shop_id) or str(body.get("partner_id")) != str(self.partner_id):
            raise ShopeeError(403, "error_auth", "Invalid partner_id or shopid.")
        now, rt = self.now(), str(body.get("refresh_token") or "")
        r = self.refresh.get(rt)
        if r is None or now >= r["expires"]:
            raise ShopeeError(403, "error_auth", "Invalid refresh_token.")
        if r["used_at"] is None:
            # the previous access tokens stay valid 5 more minutes
            for at, exp in self.access.items():
                self.access[at] = min(exp, now + OLD_ACCESS_GRACE)
            r["used_at"], r["next"] = now, self._issue()
            return dict(r["next"], partner_id=self.partner_id, shop_id=self.shop_id)
        nxt = self.refresh[r["next"]["refresh_token"]]
        if now - r["used_at"] > REPLAY or nxt["used_at"] is not None:
            raise ShopeeError(403, "error_auth", "Invalid refresh_token.")
        # a replay inside 4 hours answers the same new pair (FAQ 144)
        return dict(r["next"], partner_id=self.partner_id, shop_id=self.shop_id)

    # -- reads -------------------------------------------------------------------------------------------------------

    def order_list(self, q: dict) -> dict:
        field = q.get("time_range_field", "")
        if field not in ("create_time", "update_time"):
            raise ShopeeError(400, "error_param", "time_range_field should be create_time or update_time.")
        try:
            t0, t1, size = int(q["time_from"]), int(q["time_to"]), int(q["page_size"])
            off = int(q.get("cursor") or 0)
        except (KeyError, ValueError):
            raise ShopeeError(400, "error_param", "time_from, time_to and page_size are required integers.")
        if not (t0 < t1 and t1 - t0 <= 15 * 86400):
            raise ShopeeError(400, "order.order_list_invalid_time", "Start time must be earlier than end time and diff in 15days.")
        if not 1 <= size <= 100:
            raise ShopeeError(400, "error_param", "page_size should be between 1 and 100.")
        size = min(size, self.max_page_size)
        status = q.get("order_status")
        rows = [o for o in self.orders.values() if t0 <= o[field] <= t1 and (not status or o["order_status"] == status)]
        rows.sort(key=lambda o: (o[field], o["order_sn"]), reverse=True)
        chunk = rows[off: off + size]
        more = off + size < len(rows)
        with_status = "order_status" in (q.get("response_optional_fields") or "").split(",")
        return {"more": more, "next_cursor": str(off + size) if more else "",
                "order_list": [dict({"order_sn": o["order_sn"]}, **({"order_status": o["order_status"]} if with_status else {}))
                               for o in chunk]}

    def order_detail(self, q: dict) -> dict:
        sns = [s.strip() for s in (q.get("order_sn_list") or "").split(",") if s.strip()]
        if not 1 <= len(sns) <= 50:
            raise ShopeeError(400, "error_param", "order_sn_list should have 1 to 50 order_sn.")
        want = {f.strip() for f in (q.get("response_optional_fields") or "").split(",") if f.strip()}
        out = []
        for sn in sns:
            o = self.orders.get(sn)
            if not o:
                continue
            d = {k: o.get(k) for k in DEFAULT_FIELDS}
            d.update({k: copy.deepcopy(o[k]) for k in want if k in o and not k.startswith("_")})
            # Shopee masks buyer data for local sellers; the mock keeps none and answers masks
            if "recipient_address" in want:
                d["recipient_address"] = {k: "******" for k in ("name", "phone", "town", "district", "city", "state", "zipcode",
                                                               "full_address")} | {"region": "ID"}
            if "buyer_username" in want:
                d["buyer_username"] = "******"
            if "buyer_user_id" in want:
                d["buyer_user_id"] = 0
            out.append(d)
        return {"order_list": out}

    def escrow_list(self, q: dict) -> dict:
        try:
            t0, t1 = int(q["release_time_from"]), int(q["release_time_to"])
            size, page = int(q.get("page_size") or 40), int(q.get("page_no") or 1)
        except (KeyError, ValueError):
            raise ShopeeError(400, "error_param", "release_time_from and release_time_to are required integers.")
        if not 1 <= size <= 100 or page < 1:
            raise ShopeeError(400, "error_param", "page_size should be between 1 and 100, page_no at least 1.")
        size = min(size, self.max_page_size)
        rows = sorted((s["escrow"] for s in self.settlements.values() if t0 <= s["escrow"]["escrow_release_time"] <= t1),
                      key=lambda e: (e["escrow_release_time"], e["order_sn"]), reverse=True)
        chunk = rows[(page - 1) * size: page * size]
        return {"more": page * size < len(rows), "escrow_list": [dict(e) for e in chunk]}

    def escrow_detail(self, q: dict) -> dict:
        s = self.settlements.get(q.get("order_sn") or "")
        if not s:
            raise ShopeeError(400, "error_not_found", "Wrong parameters, detail: the order is not found.")
        return copy.deepcopy(s["detail"])

    def item_list(self, q: dict, statuses: list) -> dict:
        try:
            off, size = int(q["offset"]), int(q["page_size"])
        except (KeyError, ValueError):
            raise ShopeeError(400, "error_param", "offset and page_size are required integers.")
        if not statuses:
            raise ShopeeError(400, "error_param", "item_status is required.")
        size = min(max(1, size), 100, self.max_page_size)
        rows = [i for i in self.items if i["item_status"] in statuses]
        chunk = rows[off: off + size]
        return {"item": copy.deepcopy(chunk), "total_count": len(rows), "has_next_page": off + size < len(rows),
                "next_offset": off + len(chunk)}

    def model_list(self, q: dict) -> dict:
        try:
            item = int(q["item_id"])
        except (KeyError, ValueError):
            raise ShopeeError(400, "error_param", "item_id is required.")
        if not any(i["item_id"] == item for i in self.items):
            raise ShopeeError(400, "error_not_found", "Wrong parameters, detail: the item is not found.")
        res = copy.deepcopy(self.models.get(item) or {"tier_variation": [], "model": []})
        for m in res["model"]:
            s = self.stock[(item, m["model_id"])]
            m["stock_info_v2"]["seller_stock"][0]["stock"] = s["stock"]
            m["stock_info_v2"]["summary_info"] = {"total_reserved_stock": s["reserved"], "total_available_stock": s["stock"]}
        return res

    def tracking(self, q: dict) -> dict:
        o = self.orders.get(q.get("order_sn") or "")
        if not o:
            raise ShopeeError(400, "logistics.invalid_error", "The order is not exist.")
        return {"tracking_number": o.get("_tracking", ""), "first_mile_tracking_number": "", "last_mile_tracking_number": "",
                "plp_number": "", "hint": "", "pickup_code": ""}

    # -- writes ------------------------------------------------------------------------------------------------------

    def update_stock(self, body: dict) -> dict:
        try:
            item = int(body["item_id"])
            entries = list(body["stock_list"])
            wanted = [(int(e.get("model_id") or 0), int(e["seller_stock"][0]["stock"])) for e in entries]
        except (KeyError, ValueError, TypeError, IndexError):
            raise ShopeeError(400, "error_param", "item_id and stock_list[].seller_stock[].stock are required.")
        if not 1 <= len(wanted) <= 50:
            raise ShopeeError(400, "error_param", "stock_list should have 1 to 50 models.")
        if not any(k[0] == item for k in self.stock):
            raise ShopeeError(400, "error_not_found", "Wrong parameters, detail: the item is not found.")
        for model, n in wanted:
            s = self.stock.get((item, model))
            if s and (n < 0 or n < s["reserved"]):
                raise ShopeeError(400, "error_param", "Can not update item with stock less than reserved stock")
        ok, bad = [], []
        for model, n in wanted:
            if (item, model) not in self.stock:
                bad.append({"model_id": model, "failed_reason": "model_id is not found"})
                continue
            self.stock[(item, model)]["stock"] = n
            ok.append({"model_id": model, "location_id": "IDZ", "stock": n})
        return {"failure_list": bad, "success_list": ok}

    # -- simulate ----------------------------------------------------------------------------------------------------

    def push(self, code: int, data: dict) -> int:
        """Send one push to --push-url, signed as Shopee signs; the HTTP status (0 when unreachable or no URL)."""
        if not self.push_url:
            return 0
        body = json.dumps({"data": data, "shop_id": self.shop_id, "code": code, "timestamp": self.now()},
                          separators=(",", ":")).encode()
        req = urllib.request.Request(self.push_url, data=body, method="POST", headers={
            "Content-Type": "application/json", "Authorization": sign_push(self.partner_key, self.push_url, body)})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                st = r.status
        except urllib.error.HTTPError as e:
            st = e.code
        except OSError:
            st = 0
        self.pushes.append({"code": code, "status": st})
        return st

    def push_status(self, o: dict) -> int:
        return self.push(3, {"items": [], "ordersn": o["order_sn"], "status": o["order_status"],
                             "completed_scenario": "NORMAL" if o["order_status"] == "COMPLETED" else "",
                             "update_time": o["update_time"]})

    def _move(self, o: dict, status: str):
        o["order_status"] = status
        # update_time only goes up, also when two steps fall in the same second
        o["update_time"] = max(self.now(), o["update_time"] + 1)

    def create(self, body: dict) -> dict:
        lines = body.get("lines") or [{"item_id": 3001001, "model_id": 0, "qty": 1}]
        items = []
        for n, ln in enumerate(lines, 1):
            c = self.catalogue.get((int(ln["item_id"]), int(ln.get("model_id") or 0)))
            if not c:
                raise ShopeeError(400, "error_not_found", "Wrong parameters, detail: the item is not found.")
            items.append(dict(c, model_quantity_purchased=int(ln.get("qty") or 1), model_discounted_price=c["model_original_price"],
                              wholesale=False, weight=0.25, add_on_deal=False, main_item=False, add_on_deal_id=0,
                              promotion_type="", promotion_id=0, order_item_id=c["item_id"], promotion_group_id=0, line_item_id=n))
        self.seq += 1
        now = self.now()
        sn = f"{datetime.fromtimestamp(now, WIB):%y%m%d}SM{self.seq:05d}X"
        o = {"order_sn": sn, "region": "ID", "currency": "IDR", "cod": False, "order_status": "UNPAID", "message_to_seller": "",
             "create_time": now, "update_time": now, "days_to_ship": 2, "ship_by_date": now + 2 * 86400, "booking_sn": "",
             "item_list": items, "pay_time": None,
             "package_list": [{"package_number": f"OFGSM{self.seq:09d}", "logistics_status": "LOGISTICS_NOT_START",
                               "logistics_channel_id": 8003, "shipping_carrier": "SPX Standard", "allow_self_design_awb": False,
                               "group_shipment_id": None, "parcel_chargeable_weight_gram": 500,
                               "item_list": [{"item_id": i["item_id"], "model_id": i["model_id"], "model_quantity": i["model_quantity_purchased"],
                                              "order_item_id": i["order_item_id"], "promotion_group_id": 0, "product_location_id": "IDZ"}
                                             for i in items]}],
             "shipping_carrier": "SPX Standard", "payment_method": "", "cancel_by": "", "cancel_reason": "",
             "buyer_cancel_reason": "", "pickup_done_time": 0, "fulfillment_flag": "fulfilled_by_local_seller"}
        self.orders[sn] = o
        return o

    def pay(self, o: dict):
        if o["order_status"] != "UNPAID":
            raise ShopeeError(400, "invalid_transition", "Only an UNPAID order can be paid.")
        self._move(o, "READY_TO_SHIP")
        o["pay_time"], o["payment_method"] = o["update_time"], "ShopeePay"
        o["total_amount"] = sum(i["model_discounted_price"] * i["model_quantity_purchased"] for i in o["item_list"]) + 10000
        o["package_list"][0]["logistics_status"] = "LOGISTICS_READY"

    def ship(self, o: dict):
        if o["order_status"] not in ("READY_TO_SHIP", "PROCESSED", "RETRY_SHIP"):
            raise ShopeeError(400, "logistics.order_status_error", "logistic status not ready to ship")
        self._move(o, "SHIPPED")
        o["pickup_done_time"], o["_tracking"] = o["update_time"], tracking_for(o["order_sn"])
        o["package_list"][0]["logistics_status"] = "LOGISTICS_PICKUP_DONE"

    def complete(self, o: dict):
        if o["order_status"] not in ("SHIPPED", "TO_CONFIRM_RECEIVE"):
            raise ShopeeError(400, "invalid_transition", "Only a shipped order can be completed.")
        self._move(o, "COMPLETED")
        o["package_list"][0]["logistics_status"] = "LOGISTICS_DELIVERY_DONE"
        self.settle(o)

    def cancel(self, o: dict, status: str):
        if status not in ("CANCELLED", "IN_CANCEL") or o["order_status"] in SHIPPED or o["order_status"] == "CANCELLED":
            raise ShopeeError(400, "order.error_limit", "Can not cancel this order:order is shipped or cancelled")
        self._move(o, status)
        if status == "CANCELLED":
            o["cancel_by"], o["cancel_reason"] = "buyer", "CUSTOMER_REQUEST"
            o["package_list"][0]["logistics_status"] = "LOGISTICS_REQUEST_CANCELED"

    def settle(self, o: dict):
        """Release the order's escrow with demo rates (commission 8%, service 4%, transaction 2%, PPh 22 0.5%, an order
        processing fee of 1250, shipping 12000 of which the buyer paid 10000 and Shopee rebated 2000), by Shopee's
        escrow_amount formula over the fields that are not zero."""
        goods = sum(i["model_discounted_price"] * i["model_quantity_purchased"] for i in o["item_list"])
        orig = sum(i["model_original_price"] * i["model_quantity_purchased"] for i in o["item_list"])
        fees = {"commission_fee": goods * 8 // 100, "service_fee": goods * 4 // 100, "seller_transaction_fee": goods * 2 // 100,
                "withholding_tax": goods * 5 // 1000, "seller_order_processing_fee": 1250}
        ship_buyer, ship_actual, rebate = 10000, 12000, 2000
        escrow = goods + ship_buyer - ship_actual + rebate - sum(fees.values())
        inc = dict({k: 0 for k in ("shopee_discount", "original_shopee_discount", "voucher_from_seller", "voucher_from_shopee", "coins",
                                    "seller_coin_cash_back", "seller_return_refund", "shipping_fee_discount_from_3pl", "reverse_shipping_fee",
                                    "final_return_to_seller_shipping_fee", "buyer_transaction_fee", "campaign_fee",
                                    "order_ams_commission_fee", "shipping_seller_protection_fee_amount",
                                    "delivery_seller_protection_fee_premium_amount", "escrow_tax", "final_shipping_fee",
                                    "total_adjustment_amount")}, **fees)
        inc.update({"escrow_amount": escrow, "escrow_amount_after_adjustment": escrow, "buyer_total_amount": o.get("total_amount"),
                    "order_original_price": orig, "original_price": orig, "order_discounted_price": goods, "order_selling_price": goods,
                    "order_seller_discount": orig - goods, "seller_discount": orig - goods, "cost_of_goods_sold": goods,
                    "original_cost_of_goods_sold": goods, "buyer_paid_shipping_fee": ship_buyer, "estimated_shipping_fee": ship_actual,
                    "actual_shipping_fee": ship_actual, "shopee_shipping_rebate": rebate, "order_adjustment": [],
                    "items": [{"item_id": i["item_id"], "item_name": i["item_name"], "item_sku": i["item_sku"], "model_id": i["model_id"],
                               "model_name": i["model_name"], "model_sku": i["model_sku"],
                               "original_price": i["model_original_price"] * i["model_quantity_purchased"],
                               "discounted_price": i["model_discounted_price"] * i["model_quantity_purchased"],
                               "selling_price": i["model_discounted_price"] * i["model_quantity_purchased"],
                               "quantity_purchased": i["model_quantity_purchased"], "line_item_id": i["line_item_id"]}
                              for i in o["item_list"]]})
        esc = {"order_sn": o["order_sn"], "payout_amount": escrow, "escrow_release_time": o["update_time"]}
        self.settlements[o["order_sn"]] = {"escrow": esc, "detail": {"order_sn": o["order_sn"], "return_order_sn_list": [],
                                                                       "order_income": inc}}

    def simulate(self, what: str, sn: str, body: dict, q: dict) -> dict:
        if what == "code":
            code = _token()
            self.codes[code] = self.now() + CODE_TTL
            return {"code": code, "shop_id": self.shop_id}
        if what == "create":
            o = self.create(body)
        else:
            o = self.orders.get(sn)
            if not o:
                raise ShopeeError(404, "error_not_found", "Wrong parameters, detail: the order is not found.")
            if what == "pay":
                self.pay(o)
            elif what == "ship":
                self.ship(o)
            elif what == "complete":
                self.complete(o)
            elif what == "cancel":
                self.cancel(o, (q.get("status") or "CANCELLED").upper())
            else:
                raise ShopeeError(404, "error_not_found", "no such simulation")
        pushed = []
        if what == "ship":
            pushed.append(self.push(4, {"ordersn": o["order_sn"], "forder_id": "", "package_number": o["package_list"][0]["package_number"],
                                        "tracking_no": o["_tracking"]}))
        pushed.append(self.push_status(o))
        return {"order_sn": o["order_sn"], "order_status": o["order_status"], "update_time": o["update_time"], "pushed": pushed}


def _mask(d: dict) -> dict:
    return {k: ("***" if k in ("access_token", "refresh_token", "code", "sign") and v else v) for k, v in d.items()}


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

        def handle_any(self, method):
            u = urllib.parse.urlsplit(self.path)
            multi = urllib.parse.parse_qs(u.query, keep_blank_values=True)
            q = {k: v[0] for k, v in multi.items()}
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n) if n else b""
            try:
                body = json.loads(raw) if raw else {}
            except ValueError:
                body = None
            with m.lock:
                m.calls.append({"method": method, "path": u.path, "query": _mask(q),
                                "body": _mask(body) if isinstance(body, dict) else body})
                if u.path == "/calls" and method == "GET":
                    return self.send(200, m.calls)
                try:
                    if u.path.startswith("/simulate/"):
                        parts = u.path.split("/")
                        return self.send(200, m.simulate(parts[2], parts[3] if len(parts) > 3 else "", body or {}, q))
                    if body is None:
                        raise ShopeeError(400, "error_param", "The body is not valid JSON.")
                    m.check(u.path, q)
                    routes = {
                        ("POST", "/api/v2/auth/token/get"): lambda: m.token_get(body),
                        ("POST", "/api/v2/auth/access_token/get"): lambda: m.token_refresh(body),
                        ("GET", "/api/v2/order/get_order_list"): lambda: m.order_list(q),
                        ("GET", "/api/v2/order/get_order_detail"): lambda: m.order_detail(q),
                        ("GET", "/api/v2/payment/get_escrow_list"): lambda: m.escrow_list(q),
                        ("GET", "/api/v2/payment/get_escrow_detail"): lambda: m.escrow_detail(q),
                        ("POST", "/api/v2/product/update_stock"): lambda: m.update_stock(body),
                        ("GET", "/api/v2/product/get_item_list"): lambda: m.item_list(q, multi.get("item_status", [])),
                        ("GET", "/api/v2/product/get_model_list"): lambda: m.model_list(q),
                        ("GET", "/api/v2/logistics/get_tracking_number"): lambda: m.tracking(q),
                    }
                    fn = routes.get((method, u.path))
                    if not fn:
                        raise ShopeeError(404, "error_not_found", "Wrong parameters, detail: the api is not found.")
                    res = fn()
                    if u.path in PUBLIC_PATHS:  # token answers are flat, not under response
                        return self.send(200, dict(res, request_id=_rid(), error="", message=""))
                    return self.send(200, {"error": "", "message": "", "request_id": _rid(), "response": res})
                except ShopeeError as e:
                    return self.send(e.status, {"error": e.error, "message": e.message, "request_id": _rid()})

        def do_GET(self):
            self.handle_any("GET")

        def do_POST(self):
            self.handle_any("POST")

    return H


# ---- the relay (poll) -----------------------------------------------------------------------------------------------

def check_api(api: str) -> str:
    """The base URL the relay may send keys to: https to a host of connector.rk, or plain http to this machine."""
    u = urllib.parse.urlsplit(api)
    if (u.scheme == "https" and u.hostname in HOSTS) or (u.scheme == "http" and u.hostname in LOCAL):
        return api.rstrip("/")
    raise SystemExit(f"--api must be https://{HOSTS[0]}, https://{HOSTS[1]} or http://127.0.0.1:<port> (the mock)")


def check_rangka(rangka: str) -> str:
    """The kernel URL the relay may send RANGKA_TOKEN to: https, or plain http to this machine."""
    u = urllib.parse.urlsplit(rangka or "")
    if (u.scheme == "https" and u.hostname) or (u.scheme == "http" and u.hostname in LOCAL):
        return rangka.rstrip("/")
    raise SystemExit("--rangka must be https://<kernel> or http://127.0.0.1:<port>: RANGKA_TOKEN never goes over plain http to another machine")


class Client:
    """Signs Shopee calls and refreshes the token pair once per run when Shopee says it expired."""

    def __init__(self, api: str, partner_id, partner_key: str, shop_id, access_token: str, refresh_token: str,
                 token_file: str | None = None, now=None):
        self.api, self.partner_id, self.partner_key, self.shop_id = check_api(api), str(partner_id), partner_key, str(shop_id)
        self.access_token, self.refresh_token = access_token, refresh_token
        self.token_file, self.now = token_file, now or time.time
        self.refreshed, self.calls = False, 0

    def call(self, method: str, path: str, query: dict | None = None, body: dict | None = None, public: bool = False) -> dict:
        try:
            return self._send(method, path, query, body, public)
        except ShopeeError as e:
            if public or self.refreshed or e.error not in TOKEN_ERRORS or not self.refresh_token:
                raise
            self.do_refresh()
            return self._send(method, path, query, body, public)

    def _send(self, method, path, query, body, public) -> dict:
        ts = int(self.now())
        q = {"partner_id": self.partner_id, "timestamp": str(ts)}
        if public:
            q["sign"] = sign_public(self.partner_key, self.partner_id, path, ts)
        else:
            q.update(access_token=self.access_token, shop_id=self.shop_id)
            q["sign"] = sign_shop(self.partner_key, self.partner_id, path, ts, self.access_token, self.shop_id)
        qs = urllib.parse.urlencode(list((query or {}).items()) + list(q.items()), doseq=True)
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(f"{self.api}{path}?{qs}", data=data, method=method, headers={"Content-Type": "application/json"})
        self.calls += 1
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                doc = json.loads(r.read() or b"{}")
                status = r.status
        except urllib.error.HTTPError as e:
            try:
                doc = json.loads(e.read() or b"{}")
            except ValueError:
                doc = {}
            status = e.code
        if doc.get("error") or status >= 400:
            raise ShopeeError(status, doc.get("error") or f"http_{status}", doc.get("message") or "")
        return doc

    def do_refresh(self):
        doc = self._send("POST", "/api/v2/auth/access_token/get", None,
                         {"shop_id": int(self.shop_id), "refresh_token": self.refresh_token, "partner_id": int(self.partner_id)}, True)
        # the rotated refresh token is kept before the new access token is used (proposal part C)
        self.access_token, self.refresh_token = doc["access_token"], doc["refresh_token"]
        self.refreshed = True
        # written on every run, dry runs included: Shopee has already rotated the pair
        if self.token_file:
            write_tokens(self.token_file, {"access_token": self.access_token, "refresh_token": self.refresh_token,
                                           "expires_at": int(self.now()) + int(doc.get("expire_in") or 0)})


def write_tokens(path: str, doc: dict):
    """Write the token pair to path with mode 0600, atomically (a temp file in the same folder, then rename)."""
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".shopee-tokens-")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(doc, f)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def collect(c: Client, since_days: float = 2, page_size: int = 100, max_pages: int = 50) -> list[tuple[str, dict]]:
    """One poll: every order changed and every escrow released in the last since_days, as (flow, event)."""
    now = int(c.now())
    start = now - int(since_days * 86400)
    sns: list[str] = []
    t0 = start
    while t0 < now:  # Shopee's windows are 15 days at most
        t1 = min(now, t0 + 15 * 86400)
        cursor = ""
        for _ in range(max_pages):
            r = c.call("GET", "/api/v2/order/get_order_list", {
                "time_range_field": "update_time", "time_from": t0, "time_to": t1, "page_size": page_size,
                "cursor": cursor, "response_optional_fields": "order_status"})["response"]
            for o in r.get("order_list") or []:
                if o["order_sn"] not in sns:
                    sns.append(o["order_sn"])
            cursor = r.get("next_cursor") or ""
            if not r.get("more") or not cursor:
                break
        t0 = t1
    events: list[tuple[str, dict]] = []
    for i in range(0, len(sns), 50):
        r = c.call("GET", "/api/v2/order/get_order_detail", {
            "order_sn_list": ",".join(sns[i:i + 50]), "response_optional_fields": OPTIONAL_FIELDS})["response"]
        events += [(FLOW_STAGED, order_event(o)) for o in r.get("order_list") or []]
    escrows: list[dict] = []
    for page in range(1, max_pages + 1):
        r = c.call("GET", "/api/v2/payment/get_escrow_list", {
            "release_time_from": start, "release_time_to": now, "page_size": min(page_size, 100), "page_no": page})["response"]
        escrows += [e for e in r.get("escrow_list") or [] if e["order_sn"] not in {x["order_sn"] for x in escrows}]
        if not r.get("more"):
            break
    for e in escrows:
        s = c.call("GET", "/api/v2/payment/get_escrow_detail", {"order_sn": e["order_sn"]})["response"]
        events.append((FLOW_SETTLED, settled_event(e, s)))
    return events


def deliver(events: list[tuple[str, dict]], rangka: str, rangka_token: str) -> dict:
    """Post each event to its flow, as the kernel would start it (moka_mock.py poll does the same)."""
    out = {"events": len(events), "delivered": 0, "refused": []}
    for flow, ev in events:
        req = urllib.request.Request(f"{rangka.rstrip('/')}/api/v1/flows/{flow}/run", data=json.dumps({"event": ev}).encode(),
                                     method="POST", headers={"Content-Type": "application/json",
                                                             "Authorization": f"Bearer {rangka_token}"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                out["delivered"] += r.status == 200
        except urllib.error.HTTPError as e:
            out["refused"].append({"flow": flow, "order_id": ev.get("order_id"), "status": e.code,
                                   "body": e.read()[:300].decode(errors="replace")})
    return out


def read_tokens(token_file: str | None) -> dict:
    if token_file and os.path.exists(token_file):
        with open(token_file) as f:
            return json.load(f)
    return {}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--port", type=int, default=8931)
    s.add_argument("--fixtures", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "fixtures"))
    s.add_argument("--partner-id", type=int, default=2009001)
    s.add_argument("--shop-id", type=int, default=880001)
    s.add_argument("--day", default=None, help="move the fixtures to this day (YYYY-MM-DD)")
    s.add_argument("--max-page-size", type=int, default=100, help="cap page_size, so a small value exercises paging")
    s.add_argument("--push-url", default=None, help="the kernel hook: http://<kernel>/hooks/<org>/shopee/push_received")
    p = sub.add_parser("poll")
    p.add_argument("--api", required=True, help="the mock (http://127.0.0.1:<port>) or Shopee's sandbox or live host")
    p.add_argument("--rangka", help="the kernel's base URL (not needed with --dry-run)")
    p.add_argument("--dry-run", action="store_true", help="print the events as JSON lines, post nothing")
    p.add_argument("--since-days", type=float, default=2)
    p.add_argument("--page-size", type=int, default=100)
    p.add_argument("--token-file", default=None, help="the rotated token pair, mode 0600 (read first when it exists; "
                   "needed unless --dry-run)")
    a = ap.parse_args(argv)
    if a.cmd == "serve":
        key = os.environ.get("SHOPEE_MOCK_PARTNER_KEY", "")
        if not dummy_key(key):
            ap.error("set SHOPEE_MOCK_PARTNER_KEY to a dummy key (a part dummy or test, e.g. test-dummy-shopee-key); never a real partner key")
        mock = Mock(a.fixtures, a.partner_id, key, a.shop_id, os.environ.get("SHOPEE_MOCK_ACCESS_TOKEN", "dummy-access-token"),
                    os.environ.get("SHOPEE_MOCK_REFRESH_TOKEN", "dummy-refresh-token"), a.day, a.max_page_size, a.push_url)
        srv = ThreadingHTTPServer(("127.0.0.1", a.port), handler(mock))
        print(f"shopee mock: http://127.0.0.1:{srv.server_address[1]} (partner {a.partner_id}, shop {a.shop_id}"
              + (f", pushes to {a.push_url})" if a.push_url else ")"), flush=True)
        try:
            srv.serve_forever()
        except KeyboardInterrupt:
            pass
        return 0
    # hosts first: nothing is sent anywhere before both addresses are allowed
    api = check_api(a.api)
    if not a.dry_run:
        if not a.rangka:
            ap.error("--rangka is needed unless --dry-run")
        check_rangka(a.rangka)
        if not a.token_file:
            ap.error("--token-file is needed unless --dry-run: Shopee rotates the token pair on a refresh")
    env = {k: os.environ.get(k, "") for k in ("SHOPEE_PARTNER_ID", "SHOPEE_PARTNER_KEY", "SHOPEE_SHOP_ID",
                                               "SHOPEE_ACCESS_TOKEN", "SHOPEE_REFRESH_TOKEN", "RANGKA_TOKEN")}
    saved = read_tokens(a.token_file)
    access = saved.get("access_token") or env["SHOPEE_ACCESS_TOKEN"]
    refresh = saved.get("refresh_token") or env["SHOPEE_REFRESH_TOKEN"]
    missing = [k for k in ("SHOPEE_PARTNER_ID", "SHOPEE_PARTNER_KEY", "SHOPEE_SHOP_ID") if not env[k]]
    if not access:
        missing.append("SHOPEE_ACCESS_TOKEN")
    if not a.dry_run and not env["RANGKA_TOKEN"]:
        missing.append("RANGKA_TOKEN")
    if missing:
        sys.exit("set " + ", ".join(missing))
    c = Client(api, env["SHOPEE_PARTNER_ID"], env["SHOPEE_PARTNER_KEY"], env["SHOPEE_SHOP_ID"], access, refresh, a.token_file)
    try:
        events = collect(c, a.since_days, a.page_size)
    except ShopeeError as e:
        sys.exit(f"shopee poll: {e.error}: {e.message}")
    if a.dry_run:
        for flow, ev in events:
            print(json.dumps({"flow": flow, "event": ev}, ensure_ascii=False))
        print(json.dumps({"events": len(events), "calls": c.calls, "refreshed": c.refreshed}), file=sys.stderr)
        return 0
    out = deliver(events, a.rangka, env["RANGKA_TOKEN"])
    out.update(calls=c.calls, refreshed=c.refreshed)
    print(json.dumps(out))
    return 0 if not out["refused"] else 1


if __name__ == "__main__":
    sys.exit(main())
