# Marketplace connectors: the shared mapping

One contract for every marketplace connector in this repository (`shopee`, `tiktok_shop`; Blibli and Lazada later),
so that a new marketplace is a thin adapter: a `connector.rk` that emits the events below from the vendor's own
payloads, a mock, and one small staging flow in rangka-apps. Research and sources: Juragan's
`reports/keputusan-riset/marketplace-connectors.md` (19 Nov 2026).

Tokopedia has no connector of its own. Its Open API was shut down on 30 Sep 2025; Tokopedia orders arrive through
`tiktok_shop` with `storefront` `TOKOPEDIA`.

## Rules

1. **Rangka is the master of stock.** Stock leaves when a person submits the day's sale in Rangka. The connector pushes
   the available quantity to the marketplace (`update_stock`); a marketplace never writes stock into Rangka.
2. **An order becomes a sale when it is paid and can be shipped** (`sold`), not when it is created and not when it is
   completed. Its lines are staged and join the day's draft sale of the outlet that holds the marketplace stock: the
   path Moka sales already take in rangka-apps (`moka_sale_line`, a daily draft `outlet_sales`, a person submits).
   A flow never submits.
3. **A cancelled order** (`cancelled`) drops its staged lines, or adds a negative line when the day's sale is already
   submitted. **A return after completion** is a negative line on the day the return is accepted.
4. **Fees are booked at settlement**, from what the marketplace reports for the order: commission, service and
   transaction fees are an expense (rangka-apps account 61200, "Biaya admin pembayaran"). A voucher or discount the
   seller pays for is a sales discount on the line, never an expense. Tax the marketplace withholds (PPh 22, PMK
   37/2025) is a prepaid tax asset, never an expense.
5. **The payout clears a receivable per marketplace**; the settlement event is what the receivable is reconciled with.
6. **SKU is the join key.** The marketplace's seller SKU must equal the item's `sku` in Rangka. A line without a match
   is held as unmapped until a person maps it. There is no per-channel SKU table.
7. **No buyer data is kept.** The platforms mask names, phones and addresses, and Shopee allows personal data to be
   stored for 90 days at most. The events below carry none; the customer in Rangka is one per marketplace.
8. **One shop per marketplace per company** in this version: the connector's keys are the keys of one shop.
9. **Money is whole rupiah as the vendor reports it.** Nothing is rounded or summed in the connector; flows add up.

## Events

Every marketplace connector has these two poll triggers. Field names are the same for every marketplace; a field a
vendor does not report is emitted as null and pinned to null in `fixtures/expect.json`.

### `<connector>.order_changed`

Emitted for every order the marketplace changed in the polled window. The kernel drops an event whose emitted values
did not change (the event key is their hash), so a flow runs once per real change.

| Field | Meaning |
|---|---|
| `channel` | The connector's name: `shopee`, `tiktok_shop` |
| `storefront` | Where the buyer bought: `SHOPEE`; for `tiktok_shop` the order's `commerce_platform` (`TIKTOK_SHOP`, `TOKOPEDIA`, `TIKTOK_SHOP_APP`) |
| `order_id` | The marketplace's order number (text) |
| `status` | The vendor's own status, unchanged |
| `sold` | true while the order is paid, not cancelled, and can be or has been shipped: stage its lines |
| `shipped` | true once the parcel left the seller |
| `completed` | true once the marketplace closed the order (the settlement follows) |
| `cancelled` | true once the order is cancelled: reverse what was staged |
| `created_at`, `paid_at`, `updated_at` | Unix seconds, as the vendor reports them (`paid_at` null until paid) |
| `currency` | `IDR` |
| `buyer_total` | What the buyer paid for the order, shipping included |
| `lines` | The vendor's own list of order lines, unchanged (see "Lines per marketplace") |

`sold`, `shipped`, `completed` and `cancelled` are derived in the connector from the vendor's status, so a flow never
knows a vendor's status names. A status that is none of them (a cancellation being processed, an order on hold) changes
nothing: the flow waits for the next event.

| Flag | `shopee` (`order_status`) | `tiktok_shop` (`status`) |
|---|---|---|
| `sold` | `READY_TO_SHIP`, `PROCESSED`, `RETRY_SHIP`, `SHIPPED`, `TO_CONFIRM_RECEIVE`, `COMPLETED` | `AWAITING_SHIPMENT`, `PARTIALLY_SHIPPING`, `AWAITING_COLLECTION`, `IN_TRANSIT`, `DELIVERED`, `COMPLETED` |
| `shipped` | `SHIPPED`, `TO_CONFIRM_RECEIVE`, `COMPLETED` | `IN_TRANSIT`, `DELIVERED`, `COMPLETED` |
| `completed` | `COMPLETED` | `COMPLETED` |
| `cancelled` | `CANCELLED` | `CANCELLED` |
| none | `UNPAID`, `PENDING`, `IN_CANCEL`, `TO_RETURN`, `INVOICE_PENDING` | `UNPAID`, `ON_HOLD` |

### `<connector>.order_settled`

Emitted once the marketplace released the money of an order.

| Field | Meaning |
|---|---|
| `channel`, `order_id`, `currency` | As above |
| `settled_at` | Unix seconds: when the money was released |
| `net` | What the seller receives for the order |
| `commission_fee`, `service_fee`, `transaction_fee` | The named fee lines, with the vendor's own sign: Shopee reports a fee as a positive number, TikTok Shop as a negative one, so a flow books the absolute value. null when the vendor has no such single line (TikTok Shop has no single service fee; its program fees stay in `income`) |
| `withheld_tax` | Tax the marketplace withheld from the seller, with the vendor's sign (null when the vendor does not report it per order) |
| `income` | The vendor's own record, unchanged: other fee lines, vouchers and shipping are read from it by the flow. For `tiktok_shop` every statement transaction is emitted; only records whose `income.type` is `ORDER` are an order's settlement |

The vendor's breakdown is kept whole because marketplaces add fee lines often (Shopee added three between July and
November 2026). A flow books the three named fees and treats the rest of the gap between the sale and `net` as
"other marketplace costs" until a line is named here.

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
| `update_stock` | Set the available quantity of one SKU to an absolute number (never a delta) |
| `get_tracking` | The tracking number of a shipped order |

Arranging a shipment (pickup, drop-off, labels) stays in the seller centre in this version.

## Known limits of the kernel today

- `each:` calls its list action once, without input, so a kernel poll reads only the first page of orders or
  settlements changed in the window (100). The relay follows every page.
- A webhook trigger is a hint only: Shopee's push and TikTok Shop's webhook carry a status but no lines, and TikTok's
  order webhook says `CANCEL` where its API says `CANCELLED`. Flags come only from the polled order.

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
takes the API's address as an argument, so the same command reads the vendor's sandbox once keys exist.

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
