# Marketplace connectors: the shared mapping

One contract for every marketplace connector in this repository (`shopee`, `tiktok_shop`; Blibli and Lazada later),
so that a new marketplace is a thin adapter: a `connector.rk` that emits the events below from the vendor's own
payloads, a mock, and one small staging flow in rangka-apps. Research and sources: Juragan's
`reports/keputusan-riset/marketplace-connectors.md` (19 Nov 2026).

Tokopedia has no connector of its own. Its Open API was shut down on 30 Sep 2025; Tokopedia orders arrive through
`tiktok_shop` with `storefront` `TOKOPEDIA`.

## Rules

1. **Rangka is the master of stock.** Stock leaves Rangka when a person submits the day's sale; a marketplace never
   writes stock into Rangka. The quantity pushed to a marketplace (`update_stock`) is the stock balance in Rangka of
   the outlet that holds marketplace stock, minus every marketplace line already staged but not yet in a submitted
   sale (all channels), minus a buffer the owner sets. **A push runs only right after an order poll of that
   marketplace succeeded and every order it returned is staged**, so every marketplace sale up to the poll is already
   counted. A stock change in Rangka (a receipt, a POS sale, a submitted sale, an adjustment, a buffer change) never
   pushes by itself: it marks the SKU to be pushed after the next successful poll. If the poll fails, nothing is
   pushed, so a relay or kernel outage can never put sold units back on sale. A push is never retried later than the
   next poll: a stale push could overwrite a newer quantity, so a push that fails waits for the next poll instead.
   Every push carries its own `revision` (see "Actions").
2. **An order becomes a sale when it can be shipped** (`sold`), not when it is created and not when it is completed.
   Its lines are staged and join the day's draft sale of the outlet that holds the marketplace stock: the path Moka
   sales already take in rangka-apps (`moka_sale_line`, a daily draft `outlet_sales`, a person submits). A flow never
   submits. A prepaid order is `sold` once paid; a cash on delivery (COD) order is `sold` before any payment, with
   `paid_at` null, and its money arrives with the settlement. A COD order the buyer refuses is reversed when the
   marketplace reports it `cancelled`; if the marketplace treats it as a return instead, rule 3 applies.
3. **A cancelled order** (`cancelled`) drops its staged lines, or adds a negative line when the day's sale is already
   submitted. A `cancelled` on an order that was never staged (UNPAID to CANCELLED, or paid and cancelled within one
   poll interval) adds nothing. **Returns are not read in v0.1.** No connector reads the marketplace's returns: a
   return after completion is booked by a person from the seller centre, as a Moka refund outside its window is.
   Shopee `TO_RETURN` and TikTok Shop's returns set no flag. Reading returns is the next step ("Adding later").
   A person books a return only for an order that is `completed`: a return the marketplace accepts before
   completion ends as `cancelled` (or as a cancelled line, below) and is reversed by the flow, never by hand.
   A line the marketplace cancels inside an order that goes on (TikTok Shop `display_status` `CANCELLED` on a line,
   Shopee `cancelled_qty` on an item) is not staged; a line cancelled after it was staged is reversed with a negative
   line, keyed per line (see "Flow keys").
4. **Fees are booked at settlement**, from what the marketplace reports for the order, and only the named lines:
   `commission_fee`, `service_fee` and `transaction_fee` are an expense (rangka-apps account 61200, "Biaya admin
   pembayaran"), `withheld_tax` is a prepaid tax asset (PPh 22, PMK 37/2025), never an expense. A voucher or discount
   the seller pays for is a sales discount on the line, never an expense. Any remaining gap between the staged sale
   and what the marketplace pays (a return, a chargeback, an adjustment, a fee not named here) stays open on the
   marketplace receivable for a person to review; a flow never books it as a cost.
5. **The payout clears a receivable per marketplace**, with `adjusted_net` when the vendor reports it, else `net`. The
   settlement event is what the receivable is reconciled with.
6. **SKU is the join key.** The marketplace's seller SKU must equal the item's `sku` in Rangka. A line without a match
   is held as unmapped until a person maps it. There is no per-channel SKU table.
7. **No buyer data is kept.** The platforms mask names, phones and addresses, and Shopee allows personal data to be
   stored for 90 days at most. The events below carry none; the customer in Rangka is one per marketplace.
8. **One shop per marketplace per company** in this version: the connector's keys are the keys of one shop.
9. **Money is whole rupiah as the vendor reports it**: a number from Shopee, a decimal string from TikTok Shop. A flow
   converts with `double()` before it adds or compares, so one flow serves both. Nothing is rounded or summed in the
   connector.
10. **Each channel has a start day** in rangka-apps (like Moka's `moka_api_from`): orders paid before it are not staged,
    so sales entered by hand before the connector was switched on are not booked twice. **A sale's day** is the WIB day
    of `paid_at`; for a COD order without `paid_at`, the WIB day it became `sold`. A backlog read after an outage lands
    on the days the orders were paid, not on the day of the run.

## Events

Every marketplace connector has these two poll triggers. Field names are the same for every marketplace; a field a
vendor does not report is emitted as null and pinned to null in `fixtures/expect.json`.

### `<connector>.order_changed`

Emitted for every order the marketplace changed in the polled window, on every poll: the same order comes again on
later polls. The kernel drops an event only when every emitted value is unchanged (the event key is their hash), and
the relay (below) posts every event of the window again on every poll. So a flow never assumes one run per change:
every flow has an idempotency key ("Flow keys").

| Field | Meaning |
|---|---|
| `channel` | The connector's name: `shopee`, `tiktok_shop` |
| `storefront` | Where the buyer bought: `SHOPEE`; for `tiktok_shop` the order's `commerce_platform` (`TIKTOK_SHOP`, `TOKOPEDIA`, `TIKTOK_SHOP_APP`) |
| `order_id` | The marketplace's order number (text) |
| `status` | The vendor's own status, unchanged |
| `sold` | true while the order can be or has been shipped and is not cancelled (paid, or COD): stage its lines |
| `shipped` | true once the parcel left the seller |
| `completed` | true once the marketplace closed the order (the settlement follows) |
| `cancelled` | true once the order is cancelled: reverse what was staged |
| `created_at`, `paid_at`, `updated_at` | Unix seconds, as the vendor reports them (`paid_at` null until paid; a COD order is `sold` with `paid_at` null) |
| `currency` | `IDR` |
| `buyer_total` | What the buyer paid for the order, shipping included |
| `lines` | The vendor's own list of order lines, unchanged (see "Lines per marketplace") |

`sold`, `shipped`, `completed` and `cancelled` are derived in the connector from the vendor's status, so a flow never
knows a vendor's status names. A status that is none of them (a cancellation being processed, an order on hold, a
return) changes nothing: the flow waits for the next event.

| Flag | `shopee` (`order_status`) | `tiktok_shop` (`status`) |
|---|---|---|
| `sold` | `READY_TO_SHIP`, `PROCESSED`, `RETRY_SHIP`, `SHIPPED`, `TO_CONFIRM_RECEIVE`, `COMPLETED` | `AWAITING_SHIPMENT`, `PARTIALLY_SHIPPING`, `AWAITING_COLLECTION`, `IN_TRANSIT`, `DELIVERED`, `COMPLETED` |
| `shipped` | `SHIPPED`, `TO_CONFIRM_RECEIVE`, `COMPLETED` | `IN_TRANSIT`, `DELIVERED`, `COMPLETED` |
| `completed` | `COMPLETED` | `COMPLETED` |
| `cancelled` | `CANCELLED` | `CANCELLED` |
| none | `UNPAID`, `PENDING`, `IN_CANCEL`, `TO_RETURN`, `INVOICE_PENDING` | `UNPAID`, `ON_HOLD` |

### `<connector>.order_settled`

Emitted once the marketplace released the money of an order, and again on every poll whose window still holds it.

| Field | Meaning |
|---|---|
| `channel`, `order_id`, `currency` | As above |
| `settled_at` | Unix seconds: when the money was released |
| `net` | What the seller receives for the order |
| `adjusted_net` | What the seller receives after the marketplace's later adjustments to the order (Shopee `escrow_amount_after_adjustment`); null when the vendor does not report it per order (TikTok Shop). The receivable is cleared with `adjusted_net` when present, else `net` |
| `commission_fee`, `service_fee`, `transaction_fee` | The named fee lines, with the vendor's own sign. A fee in the vendor's cost direction is an expense; the opposite direction is a refund of that fee and reduces it. Shopee: positive is a cost; TikTok Shop: negative is a cost. Never take the absolute value. null when the vendor has no such single line (TikTok Shop has no single service fee; its program fees stay in `income`) |
| `withheld_tax` | Tax the marketplace withheld from the seller, with the vendor's sign and the same direction rule (null when the vendor does not report it per order). PPh 22 on a return: the marketplace issues a negative slip; v0.1 leaves it to the person who books the return |
| `income` | The vendor's own record, unchanged: other fee lines, vouchers and shipping are read from it. For `tiktok_shop` every statement transaction is emitted; only records whose `income.type` is `ORDER` are an order's settlement. Other records (`RESERVE`, `CHARGE_BACK`, adjustments) have an empty `order_id` and are not order settlements: a person reviews them against the receivable |

The vendor's breakdown is kept whole because marketplaces add fee lines often (Shopee added three between July and
November 2026). A flow books only the named fees and the withheld tax; whatever else separates the sale from the
payout stays open on the receivable (rule 4) until a line is named here.

### Flow keys

The kernel and the relay deliver the same event more than once, so every flow on these events MUST have an
idempotency key, and the staging flow acts on transitions only (`shipped` and `completed` book nothing in v0.1):

| Flow | Runs when (`if:`) | Idempotency key | Does |
|---|---|---|---|
| `<connector>_order_staged` | `sold` | `channel + ':' + order_id + ':sold'` | Stages the order's lines once |
| `<connector>_order_staged` | `cancelled` | `channel + ':' + order_id + ':cancelled'` | Reverses once what was staged (nothing when it was never staged, rule 3) |
| `<connector>_order_staged` | a line of a staged order is now cancelled (TikTok Shop `display_status` `CANCELLED`, Shopee `cancelled_qty` above what was reversed) | `channel + ':' + order_id + ':' + line id + ':line_cancelled'` (Shopee: `+ ':' + string(cancelled_qty)`) | Reverses that line once |
| `<connector>_order_settled` | always for `shopee`; `income.type == 'ORDER'` for `tiktok_shop` | `channel + ':' + order_id + ':settled'` | Books the named fees and withheld tax and the receivable, once |

A run whose key was seen before does nothing. A TikTok Shop record that is not `ORDER` never reaches a booking
(its `order_id` is empty, so it could not be keyed anyway).

### Lines per marketplace

A staging flow reads these fields from `lines`; nothing else differs between marketplaces.

| | `shopee` (`item_list[]`) | `tiktok_shop` (`line_items[]`) |
|---|---|---|
| SKU | `model_sku`, else `item_sku` | `seller_sku` |
| Name | `item_name`, `model_name` | `product_name`, `sku_name` |
| Quantity | `model_quantity_purchased` | 1 per entry (one entry per unit) |
| List price | `model_original_price` | `original_price` |
| Price after the seller's discount | `model_discounted_price` | `sale_price` |
| The vendor's ids for a stock push | `item_id`, `model_id` | `product_id`, `sku_id` |

## Actions every marketplace connector has

| Action | What it does |
|---|---|
| `list_orders`, `get_order_detail` (or one action when the vendor's list is complete) | Read by `order_changed` |
| `list_settlements`, `get_settlement` (or one action) | Read by `order_settled` |
| `update_stock` | Set the available quantity of rule 1 for one SKU to an absolute number, never a delta. Required input `revision`: unique for every push, increasing (the time of the push in milliseconds, as text); never the id of a stock move, because staging, cancelling and a buffer change alter the quantity without one, and the kernel keeps every key forever, so a repeated revision would drop that push. Its idempotency key is the vendor ids + `':'` + quantity + `':'` + `revision`, so a retry of the same job is sent once and every new push is sent. Pass every input explicitly (the kernel builds the key before filling input defaults). Retries are short (rule 1). Output `failures`: the vendor's per-item failure list (Shopee `failure_list`, TikTok Shop `data.errors`). The vendor answers a partial failure with success, so a flow MUST treat a non-empty `failures` as a failed push and tell the owner |
| `get_tracking` | The tracking number of a shipped order |

Arranging a shipment (pickup, drop-off, labels) stays in the seller centre in this version.

## Known limits

- Each poll reads a fixed window (orders changed in the last two days; Shopee escrows released in the last two days;
  TikTok Shop statements of the last seven days). There is no cursor: after an outage longer than the window (relay
  off, kernel down, token expired) run the relay once with `poll --since-days N` (TikTok Shop: also
  `--settled-days N`), N covering the outage. The flow keys make the overlap harmless.
- `each:` calls its list action once, without input, so a kernel poll reads only the first page of orders or
  settlements changed in the window (100). The relay follows every page of orders, statements and statement
  transactions.
- A webhook trigger is a hint only: Shopee's push and TikTok Shop's webhook carry a status but no lines, and TikTok's
  order webhook says `CANCEL` where its API says `CANCELLED`. Flags come only from the polled order.
- TikTok Shop `update_stock` sends an empty `warehouse_id` by default, read as the shop's default warehouse; to
  confirm in the Development Shop (if TikTok refuses it, the push comes back in `failures` and the flow must pass the
  warehouse id).

## Adding later

- **Returns** (the next step): a trigger per connector over Shopee `/api/v2/returns/*` (`get_return_list`,
  `get_return_detail`) and TikTok Shop `/return_refund/202602/returns/search`, emitting negative lines and the refund
  on the day the return is accepted, with PPh 22's negative slip. Until then a person books returns (rule 3).
- Adjustments and chargebacks booked by a flow (Shopee `order_adjustment[]`, TikTok Shop records that are not
  `ORDER`), instead of a person reviewing them on the receivable.

## Until the kernel signs these requests

The three large marketplaces sign a request in the query string, verify a push over a prefix and the raw body, and
refresh tokens with their own calls. The kernel (rangka-os contract 0002) signs in a header, over lines, and
refreshes with the standard OAuth form, so it cannot call them yet. Each `connector.rk` says what the kernel must do
under `experimental:` (not read by the kernel), and the proposal is Juragan's
`proposals/marketplace-connector-runtime.md`.

Until then a connector runs the way `moka` does. `<connector>/mock/<connector>_mock.py` stands in for the vendor's API
from its documented payloads and checks every signature. Its `poll` command reads the API the way the kernel's poll
trigger will (signing, paging, the token refresh), and hands each event, in the trigger's emit shape, to the tenant's
flow `<connector>_order_staged` or `<connector>_order_settled` through `POST /api/v1/flows/<name>/run`. The poller
takes the API's address as an argument, so the same command reads the vendor's sandbox once keys exist. It sends the
vendor's keys and tokens only over https to the hosts in the spec's `hosts:` (plain http only to 127.0.0.1 or
localhost, the mock), and `RANGKA_TOKEN` only over https or plain http to 127.0.0.1 or localhost. A real run needs
`--token-file` (mode 0600): the vendor rotates the token pair on a refresh, so the relay writes the new pair there on
every run that refreshed, dry runs included.

A marketplace app has one callback URL for every shop. A seller's own app (Shopee "Seller In House System", TikTok
Shop "seller developer") points it at the company's own hook. One app for many shops needs a gateway that verifies
the push and passes it on to each company, and the app's key must never be in a company's kernel (rangka-os contract
0063); that is in the proposal too.

## Adding a marketplace

1. `<name>/connector.rk`: the two triggers and the actions above, emitting exactly the fields of "Events".
2. `<name>/fixtures/`: the vendor's documented payloads with synthetic values, and `expect.json` pinning every flag
   for at least one sold, one cancelled and one in-between order.
3. `<name>/mock/<name>_mock.py` and `tests/check_<name>_mock.py`.
4. A row in "Lines per marketplace" and in the flag table.
