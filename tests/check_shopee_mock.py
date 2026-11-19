#!/usr/bin/env python3
"""Check shopee/mock/shopee_mock.py: Shopee's documented base strings, every call refused the way Shopee refuses it
(wrong sign, stale timestamp, expired token, wrong shop), the token refresh with its rotation and 4 hour replay, order
paging, no buyer data, the simulated lifecycle against the flag table of marketplace/README.md, signed pushes, absolute
stock, and the poll relay: events with exactly the emit keys of shopee/connector.rk, the same events twice, one POST
per event with the Rangka token. Stdlib only (PyYAML when importable), no network beyond 127.0.0.1.

    python3 tests/check_shopee_mock.py
"""
import contextlib
import hashlib
import io
import hmac
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "shopee", "mock"))
import shopee_mock as m  # noqa: E402

FIX = os.path.join(ROOT, "shopee", "fixtures")
SPEC = os.path.join(ROOT, "shopee", "connector.rk")
PID, KEY, SHOP, AT, RT = 2009001, "test-dummy-shopee-check", 880001, "dummy-access-check", "dummy-refresh-check"
WIB = timezone(timedelta(hours=7))
NOW = int(datetime(2026, 11, 18, 20, 0, tzinfo=WIB).timestamp())
fails = []


def check(name, cond, detail=""):
    print(("ok    " if cond else "FAIL  ") + name + ("" if cond else f"  {str(detail)[:400]}"))
    if not cond:
        fails.append(name)


def serve(h):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), h)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t


def emit_keys():
    """{trigger: [emit keys]} of shopee/connector.rk (PyYAML when importable, else the spec's own layout)."""
    try:
        import yaml
        return {t["name"]: list(t.get("emit") or {}) for t in yaml.safe_load(open(SPEC))["triggers"]}
    except ImportError:
        out, cur, in_emit = {}, None, False
        for ln in open(SPEC).read().split("\n"):
            if re.match(r"^  - name: ", ln) and cur is not False:
                cur, in_emit = ln.split(":", 1)[1].strip(), False
                out[cur] = []
            elif ln.startswith("actions:"):
                cur = False
            elif cur and ln == "    emit:":
                in_emit = True
            elif cur and in_emit:
                mm = re.match(r"^      ([a-z_]+):", ln)
                if mm:
                    out[cur].append(mm.group(1))
                elif ln.strip() and not ln.startswith("      "):
                    in_emit = False
        return out


def walk_keys(x):
    if isinstance(x, dict):
        for k, v in x.items():
            yield k
            yield from walk_keys(v)
    elif isinstance(x, list):
        for v in x:
            yield from walk_keys(v)


BUYER = {"recipient_address", "buyer_username", "buyer_user_id", "buyer_user_name", "name", "phone", "full_address"}
expect = json.load(open(os.path.join(FIX, "expect.json")))

# 1. Shopee's documented base strings (developer-guide/16) and the HMAC over them
check("shop base string = Shopee's example",
      m.base_shop(2001887, "/api/v2/shop/get_shop_info", 1655714431, "59777174636562737266615546704c6d", 14701711)
      == "2001887/api/v2/shop/get_shop_info165571443159777174636562737266615546704c6d14701711")
check("public base string = Shopee's example",
      m.base_public(2001887, "/api/v2/public/get_shops_by_partner", 1655714431) == "2001887/api/v2/public/get_shops_by_partner1655714431")
want = hmac.new(b"k-test", b"2001887/api/v2/shop/get_shop_info165571443159777174636562737266615546704c6d14701711", hashlib.sha256).hexdigest()
sig = m.sign_shop("k-test", 2001887, "/api/v2/shop/get_shop_info", 1655714431, "59777174636562737266615546704c6d", 14701711)
check("sign = lower-case hex HMAC-SHA256(partner key, base string)", sig == want and sig == sig.lower() and len(sig) == 64, sig)
body = b'{"data":{"ordersn":"X"},"shop_id":1,"code":3,"timestamp":1}'
check("push Authorization = hex HMAC-SHA256(partner key, url|body)",
      m.sign_push("k-test", "https://h.example/hook", body) == hmac.new(b"k-test", b"https://h.example/hook|" + body, hashlib.sha256).hexdigest())

# 2. the mock: refusals in Shopee's envelope
clock = Clock(NOW)
mock = m.Mock(FIX, PID, KEY, SHOP, AT, RT, clock=clock)
srv, base = serve(m.handler(mock))


def raw(path, q=None, body=None, method="GET", url=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request((url or base) + path + ("?" + urllib.parse.urlencode(q) if q else ""), data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def shop_q(path, extra=None, ts=None, token=AT, shop=SHOP, sign=None):
    ts = clock.t if ts is None else ts
    q = dict(extra or {}, partner_id=PID, timestamp=ts, access_token=token, shop_id=shop)
    q["sign"] = sign or m.sign_shop(KEY, PID, path, ts, token, shop)
    return q


P_LIST = "/api/v2/order/get_order_list"
win = {"time_range_field": "update_time", "time_from": NOW - 2 * 86400, "time_to": NOW, "page_size": 100}
st, out = raw(P_LIST, shop_q(P_LIST, win))
check("a signed call answers in Shopee's envelope", st == 200 and out["error"] == "" and out["request_id"] and "order_list" in out["response"], out)
st, out = raw(P_LIST, shop_q(P_LIST, win, sign="0" * 64))
check("wrong sign: error_sign 'Wrong sign.'", st == 403 and out["error"] == "error_sign" and out["message"] == "Wrong sign." and out["request_id"], out)
st, out = raw(P_LIST, shop_q(P_LIST, win, ts=NOW - 301))
check("timestamp older than 5 minutes: error_param 'Timestamp is expired.'", st == 400 and out == dict(out, error="error_param", message="Timestamp is expired."), out)
q = shop_q(P_LIST, win)
del q["partner_id"]
st, out = raw(P_LIST, q)
check("no partner_id: error_param", st == 400 and out["error"] == "error_param" and "partner_id" in out["message"], out)
st, out = raw(P_LIST, shop_q(P_LIST, win, shop=999))
check("another shop: error_auth", st == 403 and out["error"] == "error_auth", out)
st, out = raw(P_LIST, shop_q(P_LIST, win, token="nope"))
check("unknown access token: error_auth", st == 403 and out["error"] == "error_auth", out)
st, out = raw(P_LIST, shop_q(P_LIST, dict(win, time_from=NOW - 16 * 86400)))
check("window over 15 days: order.order_list_invalid_time", out["error"] == "order.order_list_invalid_time", out)
clock.t = NOW + m.ACCESS_TTL + 1
st, out = raw(P_LIST, shop_q(P_LIST, dict(win, time_to=clock.t)))
check("access token older than 4 hours: invalid_acceess_token", st == 403 and out["error"] == "invalid_acceess_token", out)

# 3. refresh: rotation, 5 minute grace of the old access token, 4 hour replay of the old refresh token
P_REF = "/api/v2/auth/access_token/get"


def refresh(rt, t=None):
    t = clock.t if t is None else t
    return raw(P_REF, {"partner_id": PID, "timestamp": t, "sign": m.sign_public(KEY, PID, P_REF, t)},
               {"shop_id": SHOP, "refresh_token": rt, "partner_id": PID}, "POST")


clock.t = NOW + 3600
st, r1 = refresh(RT)
check("refresh: a new pair, expire_in 14400, flat answer", st == 200 and r1["error"] == "" and r1["expire_in"] == 14400
      and r1["access_token"] != AT and r1["refresh_token"] != RT, r1)
st, out = raw(P_LIST, shop_q(P_LIST, dict(win, time_to=clock.t), token=r1["access_token"]))
check("the new access token works", st == 200, out)
st, out = raw(P_LIST, shop_q(P_LIST, dict(win, time_to=clock.t), token=AT))
check("the old access token still works within 5 minutes", st == 200, out)
clock.t += 301
st, out = raw(P_LIST, shop_q(P_LIST, dict(win, time_to=clock.t), token=AT))
check("the old access token expires 5 minutes after the refresh", st == 403 and out["error"] == "invalid_acceess_token", out)
st, r2 = refresh(RT)
check("old refresh token within 4 hours: the same new pair", st == 200 and (r2["access_token"], r2["refresh_token"]) == (r1["access_token"], r1["refresh_token"]), r2)
st, r3 = refresh(r1["refresh_token"])
check("the new refresh token rotates again", st == 200 and r3["refresh_token"] not in (RT, r1["refresh_token"]), r3)
st, out = refresh(RT)
check("old refresh token after the new one was used: refused", st == 403 and out["error"] == "error_auth", out)
clock.t += 4 * 3600 + 1
st, out = refresh(r1["refresh_token"])
check("a used refresh token after 4 hours: refused", st == 403 and out["error"] == "error_auth", out)
P_GET = "/api/v2/auth/token/get"
st, c = raw("/simulate/code", body={}, method="POST")
tq = lambda: {"partner_id": PID, "timestamp": clock.t, "sign": m.sign_public(KEY, PID, P_GET, clock.t)}  # noqa: E731
st, tok = raw(P_GET, tq(), {"code": c["code"], "shop_id": SHOP, "partner_id": PID}, "POST")
st2, again = raw(P_GET, tq(), {"code": c["code"], "shop_id": SHOP, "partner_id": PID}, "POST")
check("token/get: a code gives a pair once", st == 200 and tok["access_token"] and st2 == 403 and again["error"] == "error_auth", (tok, again))

# 4. fixtures and the mock agree; paging; no buyer data
detail = {o["order_sn"]: o for o in json.load(open(os.path.join(FIX, "get_order_detail.json")))["response"]["order_list"]}
fx = sorted(f for f in os.listdir(FIX) if f.startswith("order_changed"))
check("every order_changed fixture is an order of get_order_detail.json", all(json.load(open(os.path.join(FIX, f)))["item"] == detail[json.load(open(os.path.join(FIX, f)))["item"]["order_sn"]] for f in fx), fx)
check("order_event pins = expect.json for every order fixture",
      all(all(m.order_event(json.load(open(os.path.join(FIX, f)))["item"])[k] == v for k, v in expect[f].items()) for f in fx),
      {f: m.order_event(json.load(open(os.path.join(FIX, f)))["item"]) for f in fx})

pmock = m.Mock(FIX, PID, KEY, SHOP, AT, RT, max_page_size=2, clock=Clock(NOW))
psrv, pbase = serve(m.handler(pmock))
seen, cursor, pages = [], "", 0
while True:
    st, out = raw(P_LIST, shop_q(P_LIST, dict(win, cursor=cursor), ts=NOW), url=pbase)
    pages += 1
    seen += [o["order_sn"] for o in out["response"]["order_list"]]
    cursor = out["response"]["next_cursor"]
    if not out["response"]["more"]:
        break
check("paging (2 per page): every order once, next_cursor empty at the end",
      pages == 3 and sorted(seen) == sorted(detail) and len(seen) == len(set(seen)) and cursor == "", (pages, seen))

P_DET = "/api/v2/order/get_order_detail"
st, out = raw(P_DET, shop_q(P_DET, {"order_sn_list": ",".join(detail)}, ts=NOW), url=pbase)
o0 = out["response"]["order_list"][0]
check("detail without optional fields: Shopee's default fields only", set(o0) == set(m.DEFAULT_FIELDS), sorted(o0))
st, out = raw(P_DET, shop_q(P_DET, {"order_sn_list": ",".join(detail), "response_optional_fields": m.OPTIONAL_FIELDS}, ts=NOW), url=pbase)
check("detail with the connector's fields: no buyer field anywhere", st == 200 and not BUYER & set(walk_keys(out)), sorted(BUYER & set(walk_keys(out))))
spec_text = open(SPEC).read()
check("connector.rk and the poller ask for the same optional fields, none of them a buyer field",
      m.OPTIONAL_FIELDS in spec_text and not set(m.OPTIONAL_FIELDS.split(",")) & BUYER, m.OPTIONAL_FIELDS)
st, out = raw(P_DET, shop_q(P_DET, {"order_sn_list": ",".join(detail), "response_optional_fields": "recipient_address,buyer_username"}, ts=NOW), url=pbase)
ra = out["response"]["order_list"][0]["recipient_address"]
check("buyer fields, when asked for, come back masked (the mock keeps none)", set(ra.values()) <= {"******", "ID"}, ra)
st, out = raw(P_DET, shop_q(P_DET, {"order_sn_list": ",".join(["X"] * 51)}, ts=NOW), url=pbase)
check("detail of more than 50 orders: error_param", out["error"] == "error_param", out)

# 5. the relay: emit keys, flags, settlement, twice the same, one refresh on an expired token
keys = emit_keys()
cl = m.Client(pbase, PID, KEY, SHOP, AT, RT, now=Clock(NOW))
n0 = len(pmock.calls)
ev1 = m.collect(cl, 2, 100)
mine = pmock.calls[n0:]
staged = [e for f, e in ev1 if f == m.FLOW_STAGED]
settled = [e for f, e in ev1 if f == m.FLOW_SETTLED]
check("poll: one order_changed event per order, one order_settled per escrow", len(staged) == len(detail) and len(settled) == 1, (len(staged), len(settled)))
check("poll: order events carry exactly the emit keys of order_changed", all(list(e) == keys["order_changed"] for e in staged), (keys.get("order_changed"), list(staged[0])))
check("poll: settlement events carry exactly the emit keys of order_settled", all(list(e) == keys["order_settled"] for e in settled), (keys.get("order_settled"), list(settled[0]) if settled else None))
by_id = {e["order_id"]: e for e in staged}
check("poll: flags and values of every order = expect.json",
      all(by_id[v["order_id"]][k] == x for f, v in expect.items() if f.startswith("order_changed") for k, x in v.items()),
      {e["order_id"]: (e["sold"], e["shipped"], e["completed"], e["cancelled"]) for e in staged})
sx = json.load(open(os.path.join(FIX, "order_settled.json")))
check("poll: settlement net and fees = the fixture (and expect.json)",
      settled and all(settled[0][k] == v for k, v in expect["order_settled.json"].items()) and settled[0]["income"] == sx["item"]["order_income"]
      and settled[0]["net"] == sx["item"]["order_income"]["escrow_amount"], settled)
check("poll: no event carries buyer data", not BUYER & set(walk_keys(ev1)), sorted(BUYER & set(walk_keys(ev1))))
check("poll: followed next_cursor over every page (5 orders, 2 per page), one detail call for the batch",
      [c["query"].get("cursor", "") for c in mine if c["path"] == P_LIST] == ["", "2", "4"]
      and sum(c["path"] == P_DET for c in mine) == 1, [(c["path"], c["query"].get("cursor")) for c in mine])
check("poll: detail asked with the connector's optional fields only",
      all(c["query"].get("response_optional_fields") == m.OPTIONAL_FIELDS for c in mine if c["path"] == P_DET), mine)
ev2 = m.collect(cl, 2, 100)
check("poll twice: identical events", json.dumps(ev1, sort_keys=True) == json.dumps(ev2, sort_keys=True))

tdir = tempfile.mkdtemp()
tfile = os.path.join(tdir, "tokens.json")
pmock.clock = Clock(NOW + m.ACCESS_TTL + 60)
cl2 = m.Client(pbase, PID, KEY, SHOP, AT, RT, token_file=tfile, now=pmock.clock)
ev3 = m.collect(cl2, 2.2, 100)
saved = json.load(open(tfile)) if os.path.exists(tfile) else {}
check("expired token: one refresh, then the same events", cl2.refreshed and len(ev3) == len(ev1)
      and sum(1 for c in pmock.calls if c["path"] == P_REF) == 1, (cl2.refreshed, len(ev3)))
check("rotated tokens written to --token-file with mode 0600", saved.get("refresh_token") == cl2.refresh_token != RT
      and stat.S_IMODE(os.stat(tfile).st_mode) == 0o600, (saved.keys(), oct(os.stat(tfile).st_mode) if saved else None))
pmock.clock = Clock(NOW + 2 * m.ACCESS_TTL + 120)
dry = os.path.join(tdir, "dry.json")
rmock = m.Mock(FIX, PID, KEY, SHOP, AT, RT)   # the real clock, as the subprocess has; its access token already expired
rmock.access[AT] = 1
rsrv, rbase = serve(m.handler(rmock))
r = subprocess.run([sys.executable, m.__file__, "poll", "--api", rbase, "--dry-run", "--token-file", dry],
                   capture_output=True, text=True, timeout=60,
                   env=dict({k: v for k, v in os.environ.items() if not k.startswith(("SHOPEE_", "RANGKA_"))}, SHOPEE_PARTNER_ID=str(PID),
                            SHOPEE_PARTNER_KEY=KEY, SHOPEE_SHOP_ID=str(SHOP), SHOPEE_ACCESS_TOKEN=AT,
                            SHOPEE_REFRESH_TOKEN=RT))
dsaved = json.load(open(dry)) if os.path.exists(dry) else {}
check("dry run that refreshed: the rotated pair is written too (Shopee already rotated it), mode 0600",
      r.returncode == 0 and json.loads(r.stderr.strip().splitlines()[-1]).get("refreshed") and dsaved.get("refresh_token") not in (None, RT)
      and stat.S_IMODE(os.stat(dry).st_mode) == 0o600, (r.returncode, r.stderr[-300:]))

# 6. the simulated lifecycle, with pushes verified by a receiver
pushes = []
PUSH_PATH = "/hooks/tahu_contoh/shopee/push_received"


class Hook(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        rawb = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        url = f"http://127.0.0.1:{self.server.server_address[1]}{self.path}"
        ok = hmac.compare_digest(self.headers.get("Authorization", ""), hmac.new(KEY.encode(), url.encode() + b"|" + rawb, hashlib.sha256).hexdigest())
        pushes.append((ok, json.loads(rawb)))
        self.send_response(200 if ok else 401)
        self.send_header("Content-Length", "0")
        self.end_headers()


hsrv, hbase = serve(Hook)
lclock = Clock(NOW)
lmock = m.Mock(FIX, PID, KEY, SHOP, AT, RT, push_url=hbase + PUSH_PATH, clock=lclock)
lsrv, lbase = serve(m.handler(lmock))
lc = m.Client(lbase, PID, KEY, SHOP, AT, RT, now=lclock)


def flags(sn):
    e = next((e for f, e in m.collect(lc, 1, 100) if f == m.FLOW_STAGED and e["order_id"] == sn), None)
    return e and (e["sold"], e["shipped"], e["completed"], e["cancelled"], e["status"])


def sim(path, body=None):
    lclock.t += 60
    return raw(path, body=body or {}, method="POST", url=lbase)


st, o = sim("/simulate/create", {"lines": [{"item_id": 3001001, "model_id": 0, "qty": 2}, {"item_id": 3001002, "model_id": 4002001, "qty": 1}]})
sn = o["order_sn"]
check("create: UNPAID, nothing to stage", flags(sn) == (False, False, False, False, "UNPAID"), flags(sn))
check("create: push code 3 verified by the receiver", pushes and pushes[-1][0] and pushes[-1][1]["code"] == 3 and pushes[-1][1]["data"]["ordersn"] == sn
      and pushes[-1][1]["shop_id"] == SHOP, pushes[-1:])
sim(f"/simulate/pay/{sn}")
check("pay: READY_TO_SHIP, sold", flags(sn) == (True, False, False, False, "READY_TO_SHIP"), flags(sn))
sim(f"/simulate/ship/{sn}")
check("ship: SHIPPED, sold and shipped", flags(sn) == (True, True, False, False, "SHIPPED"), flags(sn))
check("ship: push code 4 with the tracking number, then code 3, both verified",
      [p[1]["code"] for p in pushes[-2:]] == [4, 3] and all(p[0] for p in pushes[-2:]) and pushes[-2][1]["data"]["tracking_no"] == m.tracking_for(sn), pushes[-2:])
P_TRK = "/api/v2/logistics/get_tracking_number"
st, out = raw(P_TRK, shop_q(P_TRK, {"order_sn": sn}, ts=lclock.t), url=lbase)
check("get_tracking_number answers the pushed number", out["response"]["tracking_number"] == m.tracking_for(sn), out)
sim(f"/simulate/complete/{sn}")
check("complete: COMPLETED, sold, shipped and completed", flags(sn) == (True, True, True, False, "COMPLETED"), flags(sn))
se = next((e for f, e in m.collect(lc, 1, 100) if f == m.FLOW_SETTLED and e["order_id"] == sn), {})
inc = se.get("income") or {}
formula = (inc.get("original_cost_of_goods_sold", 0) - inc.get("voucher_from_seller", 0) + inc.get("buyer_paid_shipping_fee", 0)
           - inc.get("actual_shipping_fee", 0) + inc.get("shopee_shipping_rebate", 0) - inc.get("seller_transaction_fee", 0)
           - inc.get("service_fee", 0) - inc.get("commission_fee", 0) - inc.get("withholding_tax", 0) - inc.get("seller_order_processing_fee", 0))
check("complete: a settlement event whose net follows Shopee's escrow formula", se and se["net"] == formula > 0
      and se["commission_fee"] > 0 and se["settled_at"] == lclock.t, se)
st, o2 = sim("/simulate/create")
sim(f"/simulate/pay/{o2['order_sn']}")
sim(f"/simulate/cancel/{o2['order_sn']}?status=IN_CANCEL")
check("IN_CANCEL: no flag (the flow waits)", flags(o2["order_sn"]) == (False, False, False, False, "IN_CANCEL"), flags(o2["order_sn"]))
sim(f"/simulate/cancel/{o2['order_sn']}")
check("cancel: CANCELLED, cancelled only", flags(o2["order_sn"]) == (False, False, False, True, "CANCELLED"), flags(o2["order_sn"]))
st, out = sim(f"/simulate/cancel/{sn}")
check("a shipped order cannot be cancelled", st == 400 and out["error"] == "order.error_limit", out)
check("every push verified", pushes and all(p[0] for p in pushes), [p for p in pushes if not p[0]])

# 7. update_stock: absolute, refuses below the reserved stock
P_STK, P_MOD = "/api/v2/product/update_stock", "/api/v2/product/get_model_list"


def set_stock(item, model, n):
    return raw(P_STK, shop_q(P_STK, ts=lclock.t), {"item_id": item, "stock_list": [{"model_id": model, "seller_stock": [{"stock": n}]}]}, "POST", url=lbase)


def model_stock():
    st, out = raw(P_MOD, shop_q(P_MOD, {"item_id": 3001002}, ts=lclock.t), url=lbase)
    return out["response"]["model"][0]["stock_info_v2"]["seller_stock"][0]["stock"]


st, out = set_stock(3001002, 4002001, 20)
st2, out2 = set_stock(3001002, 4002001, 20)
check("update_stock sets an absolute value (20 twice stays 20)", out["response"]["success_list"][0]["stock"] == 20 and model_stock() == 20, (out, out2))
st, out = set_stock(3001002, 4002001, 1)
check("update_stock below the reserved stock (2): refused, stock kept",
      st == 400 and out["error"] == "error_param" and "reserved stock" in out["message"] and model_stock() == 20, out)
st, out = set_stock(3001001, 0, 0)
check("update_stock of an item without variations (model_id 0) to 0", st == 200 and out["response"]["success_list"] == [{"model_id": 0, "location_id": "IDZ", "stock": 0}], out)

# 7b. update_stock in the spec: a required revision in the idempotency key (a later push of a quantity sent before
# is a new key; the same push repeated is the same key), failures in the output
stock_block = spec_text.split("  - name: update_stock\n", 1)[1].split("\n  - name: ", 1)[0]
check("spec update_stock: revision is a required input and ends the idempotency key; output failures = failure_list",
      re.search(r"\{name: revision, type: data, required: true", stock_block) is not None
      and "idempotency: string(input.item_id) + ':' + string(input.model_id) + ':' + string(input.stock) + ':' + input.revision\n" in stock_block + "\n"
      and "failures: response.failure_list" in stock_block and "as_of" not in stock_block, stock_block[-600:])
key = lambda item, model, n, rev: f"{item}:{model}:{n}:{rev}"  # noqa: E731  (the spec's key, in Python)
pushes_keys = [key(3001002, 4002001, n, rev) for n, rev in ((5, "mv-101"), (3, "mv-102"), (5, "mv-103"), (5, "mv-103"))]
check("stock keys: 5, 3, 5 again after two changes are three keys; a repeated push is one", len(set(pushes_keys)) == 3, pushes_keys)

# 8. deliver: one POST per event to the flow, Bearer RANGKA_TOKEN
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
res = m.deliver(ev1, fbase, "rk-token")
check("deliver: one request per event, all delivered", res["events"] == len(ev1) == res["delivered"] == len(got) and not res["refused"], res)
check("deliver: flows shopee_order_staged / shopee_order_settled with the Rangka token",
      all(g["auth"] == "Bearer rk-token" for g in got)
      and [g["path"] for g in got] == [f"/api/v1/flows/{f}/run" for f, _ in ev1] and got[0]["body"] == {"event": ev1[0][1]}, got[:1])

# 9. the CLI: poll --dry-run against a mock moved to yesterday, the API allow-list, and serve's key rule
yday = (datetime.now(WIB) - timedelta(days=1)).date().isoformat()
cmock = m.Mock(FIX, PID, KEY, SHOP, AT, RT, day=yday, max_page_size=2)
csrv, cbase = serve(m.handler(cmock))
env = dict({k: v for k, v in os.environ.items() if not k.startswith(("SHOPEE_", "RANGKA_"))}, SHOPEE_PARTNER_ID=str(PID),
           SHOPEE_PARTNER_KEY=KEY, SHOPEE_SHOP_ID=str(SHOP), SHOPEE_ACCESS_TOKEN=AT, SHOPEE_REFRESH_TOKEN=RT)
r = subprocess.run([sys.executable, m.__file__, "poll", "--api", cbase, "--dry-run"], capture_output=True, text=True, env=env, timeout=60)
lines = [json.loads(x) for x in r.stdout.splitlines() if x.strip()]
check("poll --dry-run: JSON lines, one per order and settlement, keys = the spec's emit keys",
      r.returncode == 0 and len(lines) == len(detail) + 1
      and all(list(x["event"]) == keys["order_changed" if x["flow"] == m.FLOW_STAGED else "order_settled"] for x in lines), (r.returncode, r.stderr[-300:], len(lines)))
check("poll output carries no token", AT not in r.stdout + r.stderr and KEY not in r.stdout + r.stderr)
r = subprocess.run([sys.executable, m.__file__, "poll", "--api", "http://203.0.113.9:8931", "--dry-run"], capture_output=True, text=True, env=env, timeout=30)
check("poll refuses plain http to another machine", r.returncode != 0 and "--api must be" in r.stderr, r.stderr[-200:])
# the relay checks every address before it sends anything: urlopen is replaced by a recorder for these runs
sent = []
real_urlopen = m.urllib.request.urlopen
m.urllib.request.urlopen = lambda req, *a, **k: sent.append(getattr(req, "full_url", req)) or (_ for _ in ()).throw(OSError("blocked"))
saved_env = dict(os.environ)
os.environ.update(env, RANGKA_TOKEN="rk-token")


def refused(args):
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            m.main(args)
        return False
    except SystemExit as e:
        return e.code not in (0, None)


try:
    tf = os.path.join(tdir, "relay.json")
    r1 = refused(["poll", "--api", "https://attacker.example", "--dry-run"])
    r2 = refused(["poll", "--api", "http://203.0.113.9:8931", "--rangka", "http://127.0.0.1:1", "--token-file", tf])
    r3 = refused(["poll", "--api", cbase, "--rangka", "http://203.0.113.9:8101", "--token-file", tf])
    r4 = refused(["poll", "--api", cbase, "--rangka", "http://127.0.0.1:1"])
finally:
    m.urllib.request.urlopen = real_urlopen
    os.environ.clear()
    os.environ.update(saved_env)
check("relay: a host outside connector.rk (https or plain http) is refused before any request is sent", r1 and r2 and not sent, sent)
check("relay: RANGKA_TOKEN over plain http to another machine is refused before any request is sent", r3 and not sent, sent)
check("relay: a run that posts refuses to start without --token-file", r4 and not sent, sent)
check("relay: https to any kernel, plain http to this machine only",
      m.check_rangka("https://rangka.example/") == "https://rangka.example" and m.check_rangka("http://localhost:8101") == "http://localhost:8101")

# --day <today> early in the morning: the newest fixture time lies at most at now, so a poll finds every fixture
early = Clock(int(datetime.now(WIB).replace(hour=7, minute=0, second=0, microsecond=0).timestamp()))
dmock = m.Mock(FIX, PID, KEY, SHOP, AT, RT, day=datetime.fromtimestamp(early.t, WIB).date().isoformat(), clock=early)
times = [o.get(k) or 0 for o in dmock.orders.values() for k in ("create_time", "update_time", "pay_time", "pickup_done_time")]
times += [s["escrow"]["escrow_release_time"] for s in dmock.settlements.values()]
dsrv, dbase = serve(m.handler(dmock))
dev = m.collect(m.Client(dbase, PID, KEY, SHOP, AT, RT, now=early), 2, 100)
check("--day today at 07:00 WIB: no fixture time after now, the poll finds every order and the settlement",
      max(times) <= early.t and len(dev) == len(detail) + 1, (max(times) - early.t, len(dev)))

check("only dummy keys", m.dummy_key(KEY) and m.dummy_key("TEST.x") and not m.dummy_key("live-partner-key") and not m.dummy_key("attest-key")
      and not m.dummy_key(""))
r = subprocess.run([sys.executable, m.__file__, "serve", "--port", "0"], capture_output=True, text=True,
                   env=dict(env, SHOPEE_MOCK_PARTNER_KEY="live-partner-key"), timeout=10)
check("the mock refuses to start with a key that is not a dummy", r.returncode == 2 and "dummy" in r.stderr, (r.returncode, r.stderr[-200:]))

print(f"shopee mock: {len(fails)} failed")
sys.exit(1 if fails else 0)
