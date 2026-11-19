#!/usr/bin/env python3
"""Check tiktok_shop/mock/tiktok_shop_mock.py against TikTok Shop's documented signature vectors, its error codes, its
token refresh and paging, and the shared marketplace contract (marketplace/README.md): the events the poll relay
builds have exactly the keys the triggers of tiktok_shop/connector.rk emit, with the flags of the contract, no buyer
data, and reach the flows the way moka_mock.py poll sends them. Stdlib only, no network beyond 127.0.0.1.

    python3 tests/check_tiktok_shop_mock.py
"""
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tiktok_shop", "mock"))
import tiktok_shop_mock as m  # noqa: E402

FX = os.path.join(ROOT, "tiktok_shop", "fixtures")
APP_KEY, SECRET = "demo-app-key", "tts-test-dummy-secret"
ACCESS, REFRESH = "demo-tts-access-token", "demo-tts-refresh-token"
fails = []


def check(name, cond, detail=""):
    print(("ok    " if cond else "FAIL  ") + name + ("" if cond else f"  {str(detail)[:400]}"))
    if not cond:
        fails.append(name)


def fixture(name):
    return json.load(open(os.path.join(FX, name)))


# 1. TikTok Shop's documented vectors (partner.tiktokshop.com, "Sign your API request" and "Webhooks overview")
check("request signature = TikTok's worked example",
      m.sign_request("e59af819cc", "/authorization/202309/shops", {"app_key": "29a39d", "timestamp": "1623812664"})
      == "b596b73e0cc6de07ac26f036364178ab16b0a907af13d43f0a0cd2345f582dc8")
check("sign and access_token are left out of the signed query",
      m.sign_request("e59af819cc", "/authorization/202309/shops", {"app_key": "29a39d", "timestamp": "1623812664", "sign": "x",
                                                                    "access_token": "y"})
      == "b596b73e0cc6de07ac26f036364178ab16b0a907af13d43f0a0cd2345f582dc8")
vec = ('{"type":1,"tts_notification_id":"7380066284010030890","shop_id":"7495540735365777507","timestamp":1718305585,'
       '"data":{"is_on_hold_order":true,"order_id":"576653688135258178","order_status":"UNPAID","update_time":1718305585}}')
check("webhook signature = TikTok's test vector",
      m.sign_webhook("abcdef", "123", vec) == "5dec0f11ec2f6783b8deee53c9ffbf8d024302f7c7e7fa55a35d17629031ac05")
check("the request body is signed (POST)", m.sign_request(SECRET, "/p", {"a": "1"}, b"{}") != m.sign_request(SECRET, "/p", {"a": "1"}))

# 2. a receiver standing in for the kernel hook (verifies what it gets) and a stub of the flows endpoint
pushes, runs = [], []


class Hook(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        if self.path.startswith("/api/v1/flows/"):
            runs.append({"path": self.path, "auth": self.headers.get("Authorization"), "body": json.loads(raw)})
            self.send_response(200)
            self.end_headers()
            return
        ok = self.headers.get("Authorization") == m.sign_webhook(APP_KEY, SECRET, raw)
        pushes.append((ok, json.loads(raw)))
        self.send_response(200 if ok else 401)
        self.end_headers()


hook = ThreadingHTTPServer(("127.0.0.1", 0), Hook)
threading.Thread(target=hook.serve_forever, daemon=True).start()
hook_base = f"http://127.0.0.1:{hook.server_address[1]}"
state = m.State(FX, APP_KEY, SECRET, ACCESS, REFRESH, webhook_url=hook_base + "/hooks/tahu_contoh/tiktok_shop/webhook_received",
                max_page_size=2)
mock = ThreadingHTTPServer(("127.0.0.1", 0), m.handler(state))
threading.Thread(target=mock.serve_forever, daemon=True).start()
base = f"http://127.0.0.1:{mock.server_address[1]}"
CIPHER = fixture("list_shops.json")["data"]["shops"][0]["cipher"]


def raw_call(method, path, query=None, body=None, token=ACCESS, ts=None, sign=None, cipher=CIPHER):
    q = {"app_key": APP_KEY, "timestamp": str(ts if ts is not None else int(time.time()))}
    if cipher:
        q["shop_cipher"] = cipher
    q.update(query or {})
    data = json.dumps(body, separators=(",", ":")).encode() if body is not None else b""
    q["sign"] = sign or m.sign_request(SECRET, path, q, data)
    req = urllib.request.Request(f"{base}{path}?{urllib.parse.urlencode(q)}", data=data if body is not None else None,
                                 method=method, headers={"x-tts-access-token": token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def post(path, body=b""):
    req = urllib.request.Request(base + path, data=body if isinstance(body, bytes) else json.dumps(body).encode(), method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


# 3. the gate: TikTok's errors, in its envelope
st, out = raw_call("GET", "/authorization/202309/shops", cipher=None)
check("a signed call answers code 0 with the shop and its cipher", st == 200 and out["code"] == 0
      and out["data"]["shops"][0]["cipher"] == CIPHER and set(out) == {"code", "message", "request_id", "data"}, out)
st, out = raw_call("GET", "/authorization/202309/shops", cipher=None, sign="0" * 64)
check("a wrong sign is refused: 106001", out["code"] == 106001 and st == 401, (st, out))
st, out = raw_call("GET", "/authorization/202309/shops", cipher=None, ts=int(time.time()) - 301)
check("a timestamp more than 5 minutes old is refused: 36009004", out["code"] == 36009004 and "timestamp" in out["message"].lower(), out)
st, out = raw_call("GET", "/authorization/202309/shops", cipher=None, ts=int(time.time()) + 31)
check("a timestamp more than 30 seconds ahead is refused: 36009004", out["code"] == 36009004, out)
st, out = raw_call("GET", "/authorization/202309/shops", cipher=None, token="not-a-token")
check("an unknown access token is refused: 101000", out["code"] == 101000, out)
st, out = raw_call("POST", "/order/202309/orders/search", {"page_size": "2"}, {}, cipher=None)
check("a shop call without shop_cipher is refused: 106013", out["code"] == 106013, out)
now_ts = int(time.time())
st, out = raw_call("POST", "/order/202309/orders/search", {"page_size": "2"}, {"update_time_ge": 0}, ts=now_ts)
signed_q = {"app_key": APP_KEY, "timestamp": str(now_ts), "shop_cipher": CIPHER, "page_size": "2"}
other_body = m.sign_request(SECRET, "/order/202309/orders/search", signed_q, b'{"update_time_ge":1}')
st2, out2 = raw_call("POST", "/order/202309/orders/search", {"page_size": "2"}, {"update_time_ge": 0}, ts=now_ts, sign=other_body)
check("a POST signed over another body is refused: 106001", out["code"] == 0 and out2["code"] == 106001, (out, out2))

# 4. expiry and refresh (rotation: the old refresh token stops working)
post("/simulate/expire_token")
st, out = raw_call("GET", "/authorization/202309/shops", cipher=None)
check("an expired access token is refused: 105002", out["code"] == 105002, out)
tmp = tempfile.mkdtemp()
token_file = os.path.join(tmp, "tts-tokens.json")
client = m.Client(base, base, {"app_key": APP_KEY, "app_secret": SECRET, "shop_cipher": CIPHER, "access_token": ACCESS,
                               "refresh_token": REFRESH}, token_file)
shops = client.call("GET", "/authorization/202309/shops", shop=False).get("shops")
check("the client refreshes once on 105002 and sends the call again", client.refreshed and shops and shops[0]["cipher"] == CIPHER,
      client.c.get("access_token", "")[:5])
saved = json.load(open(token_file)) if os.path.exists(token_file) else {}
check("rotated tokens are written to the token file, mode 0600",
      saved.get("refresh_token") == client.c["refresh_token"] != REFRESH and stat.S_IMODE(os.stat(token_file).st_mode) == 0o600,
      oct(stat.S_IMODE(os.stat(token_file).st_mode)) if saved else "no file")
q = urllib.parse.urlencode({"app_key": APP_KEY, "app_secret": SECRET, "refresh_token": REFRESH, "grant_type": "refresh_token"})
try:
    urllib.request.urlopen(f"{base}/api/v2/token/refresh?{q}", timeout=10)
    old = {"code": 0}
except urllib.error.HTTPError as e:
    old = json.loads(e.read())
check("the old refresh token is refused after rotation", old["code"] != 0, old)
TOKEN = client.c["access_token"]
_, code = post("/simulate/authorize")
q = {"app_key": APP_KEY, "app_secret": SECRET, "auth_code": code["auth_code"], "grant_type": "authorized_code"}
got = json.loads(urllib.request.urlopen(f"{base}/api/v2/token/get?{urllib.parse.urlencode(q)}", timeout=10).read())
try:
    urllib.request.urlopen(f"{base}/api/v2/token/get?{urllib.parse.urlencode(q)}", timeout=10)
    again = {"code": 0}
except urllib.error.HTTPError as e:
    again = json.loads(e.read())
check("token/get trades an auth code once (grant_type authorized_code), with expiry as Unix seconds",
      got["code"] == 0 and got["data"]["access_token_expire_in"] > time.time() and again["code"] == 36004004, (got.get("code"), again))

# 5. paging: every order once
ids, n_pages, tok = [], 0, ""
while True:
    st, out = raw_call("POST", "/order/202309/orders/search", {"page_size": "2", "page_token": tok, "sort_field": "update_time",
                                                               "sort_order": "ASC"}, {"update_time_ge": 0}, token=TOKEN)
    n_pages += 1
    ids += [o["id"] for o in out["data"]["orders"]]
    tok = out["data"]["next_page_token"]
    if not tok or n_pages > 20:
        break
want = sorted(fixture(os.path.basename(f))["item"]["id"] for f in __import__("glob").glob(os.path.join(FX, "order_changed*.json")))
check("order search pages over page_token and returns every order once", sorted(ids) == want and n_pages == 3, (n_pages, ids))

# 6. no buyer data in detail answers or events
BUYER = {"recipient_address", "buyer_nickname", "buyer_email", "buyer_message", "phone_number", "full_address", "first_name",
         "last_name", "user_id"}


def keys(o):
    if isinstance(o, dict):
        return set(o) | set().union(*(keys(v) for v in o.values())) if o else set()
    if isinstance(o, list):
        return set().union(*(keys(v) for v in o)) if o else set()
    return set()


st, out = raw_call("GET", "/order/202507/orders", {"ids": ",".join(want)}, token=TOKEN)
check("order detail answers every id and carries no buyer name, phone or address",
      out["code"] == 0 and len(out["data"]["orders"]) == len(want) and not keys(out) & BUYER, keys(out) & BUYER)
st, out = raw_call("GET", "/order/202507/orders", {"ids": "999"}, token=TOKEN)
check("an order of another seller: 21008111", out["code"] == 21008111, out)


# 7. poll --dry-run as a command: event keys = the emit keys of connector.rk
def emit_keys(trigger):
    text = open(os.path.join(ROOT, "tiktok_shop", "connector.rk")).read()
    try:
        import yaml
        spec = yaml.safe_load(text)
        return set(next(t for t in spec["triggers"] if t["name"] == trigger)["emit"])
    except ImportError:
        block = re.search(rf"\n  - name: {trigger}\n(.*?)(?=\n  - name: |\nactions:)", text, re.S).group(1)
        emit = block.split("\n    emit:\n", 1)[1]
        return set(re.findall(r"^      ([a-z_]+):", emit, re.M))


env = dict(os.environ, TIKTOK_SHOP_APP_KEY=APP_KEY, TIKTOK_SHOP_APP_SECRET=SECRET, TIKTOK_SHOP_SHOP_CIPHER=CIPHER,
           TIKTOK_SHOP_ACCESS_TOKEN=TOKEN, TIKTOK_SHOP_REFRESH_TOKEN=client.c["refresh_token"])
env.pop("RANGKA_TOKEN", None)


def dry_run():
    r = subprocess.run([sys.executable, m.__file__, "poll", "--api", base, "--auth-api", base, "--dry-run", "--page-size", "2"],
                       capture_output=True, text=True, env=env, timeout=60)
    return r, [json.loads(x) for x in r.stdout.splitlines() if x.strip()]


r, lines = dry_run()
staged = [x["event"] for x in lines if x["flow"] == "tiktok_shop_order_staged"]
settled = [x["event"] for x in lines if x["flow"] == "tiktok_shop_order_settled"]
check("poll --dry-run runs and prints one JSON line per event", r.returncode == 0 and len(staged) == len(want) and len(settled) == 1,
      (r.returncode, r.stderr[-300:], len(staged), len(settled)))
check("order_changed events have exactly the emit keys of the spec", staged and all(set(e) == emit_keys("order_changed") for e in staged),
      [sorted(set(e) ^ emit_keys("order_changed")) for e in staged[:1]])
check("order_settled events have exactly the emit keys of the spec", settled and set(settled[0]) == emit_keys("order_settled"),
      sorted(set(settled[0]) ^ emit_keys("order_settled")) if settled else "none")
check("the events carry no buyer data", not keys(lines) & BUYER, keys(lines) & BUYER)
expect = fixture("expect.json")
by_id = {e["order_id"]: e for e in staged}
bad = []
for f, pins in expect.items():
    if not f.startswith("order_changed"):
        continue
    e = by_id.get(fixture(f)["item"]["id"], {})
    for k in ("sold", "shipped", "completed", "cancelled", "storefront", "status", "buyer_total", "channel"):
        if k in pins and e.get(k) != pins[k]:
            bad.append((f, k, e.get(k), pins[k]))
check("flags, storefront and status of every polled fixture order match expect.json", not bad and by_id, bad)
tok_ev = by_id.get(fixture("order_changed_tokopedia.json")["item"]["id"], {})
check("a Tokopedia order emits storefront TOKOPEDIA and channel tiktok_shop",
      tok_ev.get("storefront") == "TOKOPEDIA" and tok_ev.get("channel") == "tiktok_shop", tok_ev)
fx_settled = fixture("order_settled.json")
pins = expect["order_settled.json"]
s = settled[0] if settled else {}
check("the settlement event's net and fees equal the fixture",
      all(s.get(k) == pins[k] for k in ("net", "commission_fee", "service_fee", "transaction_fee", "withheld_tax", "order_id", "currency"))
      and s.get("income", {}).get("fee_tax_breakdown") == fx_settled["item"]["fee_tax_breakdown"], {k: s.get(k) for k in pins})
r2, lines2 = dry_run()
check("a second identical poll gives identical events", r2.returncode == 0 and lines2 == lines, len(lines2))

# 8. poll posting to a stub of the flows endpoint: bearer token, one request per event
env_post = dict(env, RANGKA_TOKEN="rk-demo-token")
r = subprocess.run([sys.executable, m.__file__, "poll", "--api", base, "--auth-api", base, "--rangka", hook_base], capture_output=True,
                   text=True, env=env_post, timeout=60)
res = json.loads(r.stdout or "{}")
check("poll posts one request per event to /api/v1/flows/<name>/run with the bearer token",
      r.returncode == 0 and len(runs) == len(lines) == res.get("delivered") and all(x["auth"] == "Bearer rk-demo-token" for x in runs)
      and {x["path"] for x in runs} == {"/api/v1/flows/tiktok_shop_order_staged/run", "/api/v1/flows/tiktok_shop_order_settled/run"}
      and [x["body"]["event"] for x in runs] == [x["event"] for x in lines], (r.returncode, r.stderr[-300:], len(runs), res))
check("the relay never prints a secret or a token", all(t not in r.stdout + r.stderr for t in (SECRET, TOKEN, "rk-demo-token")))

# 9. the simulated lifecycle changes the flags as the contract says, and every push verifies at the receiver
poll_client = m.Client(base, base, dict(client.c), None)


def event_of(oid):
    return next((e for f, e in m.collect(poll_client, page_size=2) if f == m.FLOW_STAGED and e["order_id"] == oid), {})


def flags(e):
    return (e.get("sold"), e.get("shipped"), e.get("completed"), e.get("cancelled"))


st, out = post("/simulate/create", {"commerce_platform": "TOKOPEDIA", "skus": ["TH-ORI", "TH-ORI", "TH-PDS"]})
oid = out["order"]["id"]
e = event_of(oid)
check("create: UNPAID, no flag, storefront TOKOPEDIA, one line per unit", flags(e) == (False, False, False, False)
      and e["storefront"] == "TOKOPEDIA" and len(e["lines"]) == 3 and e["paid_at"] is None, e)
check("create pushes webhook type 1, signed so the receiver recomputes it", out["notified"] == 200 and pushes and pushes[-1][0]
      and pushes[-1][1]["type"] == 1 and pushes[-1][1]["data"]["order_id"] == oid and pushes[-1][1]["data"]["order_status"] == "UNPAID",
      pushes[-1:])
steps = [("hold", "ON_HOLD", (False, False, False, False)), ("pay", "AWAITING_SHIPMENT", (True, False, False, False)),
         ("ship", "IN_TRANSIT", (True, True, False, False)), ("deliver", "DELIVERED", (True, True, False, False)),
         ("complete", "COMPLETED", (True, True, True, False))]
prev = e["updated_at"]
for act, status, want_flags in steps:
    st, out = post(f"/simulate/{act}/{oid}")
    e = event_of(oid)
    check(f"{act}: {status}, flags {want_flags}, update_time moves, push verifies",
          st == 200 and e["status"] == status and flags(e) == want_flags and e["updated_at"] > prev and out["notified"] == 200
          and pushes[-1][0] and pushes[-1][1]["data"]["order_status"] == status, (st, e.get("status"), flags(e), out.get("notified")))
    prev = e["updated_at"]
paid_settled = [x for f, x in m.collect(poll_client, page_size=2) if f == m.FLOW_SETTLED and x["order_id"] == oid]
check("complete makes a paid statement with this order's settlement", len(paid_settled) == 1 and int(paid_settled[0]["net"]) > 0
      and int(paid_settled[0]["commission_fee"]) < 0, paid_settled)
pkg = out["order"]["packages"][0]["id"]
st, trk = raw_call("GET", f"/fulfillment/202309/packages/{pkg}", token=TOKEN)
check("get_tracking: the shipped package's tracking number", trk["code"] == 0 and trk["data"]["tracking_number"].startswith("JX")
      and trk["data"]["orders"] == [{"id": oid}], trk)
st, out = post("/simulate/create", {"commerce_platform": "TIKTOK_SHOP", "skus": ["TH-PDS"]})
oid2 = out["order"]["id"]
post(f"/simulate/pay/{oid2}")
st, out = post(f"/simulate/cancel/{oid2}")
e = event_of(oid2)
check("cancel after payment: CANCELLED, only cancelled; the push says CANCEL (the API says CANCELLED)",
      flags(e) == (False, False, False, True) and e["status"] == "CANCELLED" and pushes[-1][1]["data"]["order_status"] == "CANCEL", e)
st, out = post(f"/simulate/ship/{oid2}")
check("a cancelled order cannot be shipped (409)", st == 409, (st, out))

# 10. stock: absolute values
prod = fixture("list_products.json")["data"]["products"][0]
pid, sid, wh = prod["id"], prod["skus"][0]["id"], prod["skus"][0]["inventory"][0]["warehouse_id"]


def qty():
    _, out = raw_call("POST", "/product/202502/products/search", {"page_size": "10"}, {"status": "ACTIVATE", "seller_skus": ["TH-ORI"]},
                      token=TOKEN)
    ps = out["data"]["products"]
    return ps[0]["skus"][0]["inventory"][0]["quantity"] if len(ps) == 1 else ("products", len(ps))


body = {"skus": [{"id": sid, "inventory": [{"warehouse_id": wh, "quantity": 7}]}]}
a1 = raw_call("POST", f"/product/202309/products/{pid}/inventory/update", body=body, token=TOKEN)[1]
q1 = qty()
raw_call("POST", f"/product/202309/products/{pid}/inventory/update", body=body, token=TOKEN)
q2 = qty()
raw_call("POST", f"/product/202309/products/{pid}/inventory/update", body={"skus": [{"id": sid, "inventory": [{"warehouse_id": "", "quantity": "3"}]}]},
         token=TOKEN)
q3 = qty()
check("update_stock sets an absolute quantity (7, 7 again stays 7, then 3 in the default warehouse)",
      a1["code"] == 0 and a1["data"] == {} and (q1, q2, q3) == (7, 7, 3), (a1, q1, q2, q3))
_, bad_wh = raw_call("POST", f"/product/202309/products/{pid}/inventory/update",
                     body={"skus": [{"id": sid, "inventory": [{"warehouse_id": "999", "quantity": 1}]}]}, token=TOKEN)
check("an unknown warehouse is a partial failure: code 0 with data.errors 12052097",
      bad_wh["code"] == 0 and bad_wh["data"]["errors"][0]["code"] == 12052097 and qty() == 3, bad_wh)

# 11. the mock takes dummy secrets only, from the environment
check("only dummy secrets", m.dummy_key(SECRET) and m.dummy_key("x.TEST.y") and not m.dummy_key("e59af819cc")
      and not m.dummy_key("attestation-secret") and not m.dummy_key(""))
env_bad = {k: v for k, v in os.environ.items() if k != "TIKTOK_SHOP_MOCK_APP_SECRET"}
env_bad["TIKTOK_SHOP_MOCK_APP_SECRET"] = "partner-center-secret"
r = subprocess.run([sys.executable, m.__file__, "serve", "--port", "0"], capture_output=True, text=True, env=env_bad, timeout=10)
check("the mock refuses to start with a secret that is not a dummy", r.returncode == 2 and "dummy" in r.stderr
      and "partner-center-secret" not in r.stderr, (r.returncode, r.stderr[-200:]))
calls = json.loads(urllib.request.urlopen(base + "/calls", timeout=10).read())
check("/calls never shows a secret or a token", not any(t in json.dumps(calls) for t in (SECRET, TOKEN, REFRESH)))

print(f"tiktok_shop mock: {len(fails)} failed")
sys.exit(1 if fails else 0)
