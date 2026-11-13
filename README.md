# Rangka Connectors

**Plug-and-play integrations for [Rangka OS](https://github.com/rangka-os/rangka-os)**, each declared in one
`connector.rk` file: how to authenticate, which events come in (webhooks, polling), which actions go out, how fields
map, and how retries and duplicates are handled. Connectors are deterministic: no LLM sits in the data path.

> **Status: v0 (October 2026).** The format and its schema are done, with WhatsApp Business as the first connector.
> The kernel runs connectors (contract 0002): webhooks at `/hooks/<org>/<connector>/<trigger>`, polls on a schedule,
> and a flow's `call` step as a queued job. Keys are set per org and never live in these files (contract 0017).

Part of the Rangka family: [rangka-os](https://github.com/rangka-os/rangka-os) ·
[rangka-apps](https://github.com/rangka-os/rangka-apps) · **rangka-connectors** ·
[rangka-ai](https://github.com/rangka-os/rangka-ai)

## Connectors

| Connector | Service | Triggers | Actions | Status |
| --- | --- | --- | --- | --- |
| [`whatsapp_business`](whatsapp_business/connector.rk) | Meta WhatsApp Cloud API | `message_received`, `message_status` (webhook, `X-Hub-Signature-256` checked) | `send_template` (with retries); collection templates in [templates.md](whatsapp_business/templates.md) | Spec |
| [`xendit`](xendit/connector.rk) | Xendit (VA, QRIS, payment links; xenPlatform sub-accounts) | `va_paid`, `qr_paid`, `invoice_paid` (webhook, `x-callback-token` checked; every callback, `paid` marks a completed payment) | `create_fixed_va`, `create_qr_code`, `create_invoice`, `get_invoice` (with retries, idempotent on the invoice number and amount) | Spec (local test-mode mock `xendit/mock/xendit_mock.py`: fixed VA and dynamic QRIS, simulated payments send the callbacks with `x-callback-token`, redelivery for idempotency tests; dummy keys only, from `XENDIT_MOCK_SECRET_KEY` and `XENDIT_MOCK_CALLBACK_TOKEN`; checked by `tests/check_xendit_mock.py`; no account used) |
| [`doku`](doku/connector.rk) | DOKU Checkout, Non-SNAP (QRIS and payment links) | `payment_notified` (webhook, DOKU's HMACSHA256 signature over Client-Id, Request-Id, Request-Timestamp, Request-Target and Digest checked by the kernel; `paid` only for status SUCCESS of a SALE, or of a notification without a type; a REFUND is never paid) | `create_checkout` (signed with `auth.sign`, idempotent on invoice, amount and `attempt`: a new page after the old one expired is the next attempt) | Spec (needs rangka-os `auth.sign` / `verify.signature.lines`, contract 0002 refinement 11 Nov 2026; synthetic fixtures in `doku/fixtures`; local mock `doku/mock/doku_mock.py` (dummy keys only, from `DOKU_MOCK_SECRET_KEY`: it signs whatever it is asked), checked by `tests/check_doku_mock.py`; no account used) |
| [`loyverse`](loyverse/connector.rk) | Loyverse POS API v1.0 (personal access token) | `receipt_updated`, `inventory_level_updated` (webhooks) | `list_receipts`, `list_stores`, `list_items`, `create_webhook` | Spec (fields from the public API reference, not checked against a live account) |
| [`moka`](moka/connector.rk) | Moka POS Connect API (OAuth 2.0) | `transaction_created` (poll every 5 min of the transactions Moka received in the last two days, newest first, every page through `next_url`, so late refunds and offline syncs come in again; an outlet moves from the export file to the API from its `moka_api_from` day in rangka-apps) | `latest_transactions`, `list_outlets`, `list_items` | Spec (paths to confirm once Moka approves API access; synthetic fixtures for two outlets, a refund at sale time, a refund made the next day and the per-outlet `each:` item in `moka/fixtures`) |
| [`biteship`](biteship/connector.rk) | Biteship shipping aggregator (JNE, J&T, SiCepat, AnterAja, GoSend, Grab and more) | `order_status`, `order_price`, `order_waybill` (webhooks, `x-biteship-secret` header checked; `delivered`/`failed`/`on_the_way` derived from the status) | `search_areas`, `get_rates`, `create_order` (idempotent on the reference), `get_order`, `cancel_order`, `get_tracking` | Spec (public API reference; synthetic fixtures in `biteship/fixtures`, checked by `tests/check_fixtures.py`; no account used) |

Specs with a `fixtures/` folder are checked without network or account: `python3 tests/check_fixtures.py` (needs PyYAML) resolves
every emit and output against the synthetic payloads and the values pinned in `fixtures/expect.json`.
`python3 tests/check_doku_mock.py` (stdlib) checks the DOKU mock: its signatures equal the vectors the kernel is
tested with, a simulated payment reaches a hook signed so that the receiver recomputes it, and a simulated refund
arrives as a signed notification of type REFUND (never money in).
`python3 tests/check_xendit_mock.py` (stdlib) checks the Xendit mock: Basic auth, one VA per invoice, a closed or single-use VA
and a dynamic QR paid once, and callbacks that carry the token and every field the connector's triggers emit from.
`moka/mock/moka_mock.py` stands in for the Moka API (outlets and latest transactions from fixtures, Bearer token, the
`since` window, `next_url` pages, `--day` to move a fixture to any date) and its `poll` command reads it the way the
kernel's poll trigger does and hands each event, in the trigger's emit shape, to the tenant's flow
`moka_transaction_staged` (the kernel calls `api.mokapos.com` only, so a local kernel cannot poll the mock itself).
`python3 tests/check_moka_mock.py` (stdlib) checks it. Juragan's local end-to-end demo uses the three mocks with dummy keys.

Until the kernel runs connectors, the retail and F&B apps read POS data from export files (rangka-apps
`moka_item_details`, `loyverse_receipts`, `pos_sales_file`); the two POS specs above are what a scheduled flow will call.

Planned, in rough order: QRIS (Midtrans), bank statements, e-Faktur / Coretax, Tokopedia, Shopee, TikTok Shop,
GoFood / GrabFood, Accurate / Jurnal import.

A payment connector ends in a `payment` whose `gateway_ref` is held by one live payment at a time in rangka-apps
(`gateway_ref_claim`): one invoice is never paid twice by a repeated callback, and a cancelled payment gives its id
back. The flow below is for `xendit.va_paid`; the same steps serve `qr_paid` and `invoice_paid`, whose callbacks also
arrive for non-payments (an `EXPIRED` link, a QR that did not succeed), which is what `if: event.paid` filters out.
rangka-apps ships these steps as `modules/payments/flows/payment_from_xendit_{va,qr,invoice}.rk` (ready, not active until
the kernel runs connectors; day_one feeds them synthetic callbacks through `POST /api/v1/flows/<name>/run`).

```yaml
# a site flow until ports land (contract 0002 §B); modules will call payment.* ports instead of xendit.*
on: xendit.va_paid
if: event.paid            # the trigger fires for every callback; only a completed payment records money
idempotency: event.gateway_ref
steps:
  # a live payment already holds this gateway id: the callback came again, do nothing (the flow's `idempotency:` is
  # read but not enforced by the kernel yet, so this step and the claim below are the guard)
  - {find: payment, where: "gateway_ref == event.gateway_ref && docstatus != 2", as: done}
  # only a submitted invoice (docstatus 1): a callback for a cancelled invoice (or its amended "-1") records nothing
  - {find: sales_invoice, where: "name == event.external_id && docstatus == 1", as: inv}
  # drafts already made for this invoice have not lowered outstanding yet: allocate only what is still open after them
  - {find_all: payment, where: "docstatus == 0 && reference == event.external_id", as: drafts}
  - create: payment
    if: done == null && inv != null
    set:
      direction: "'receive'"
      customer: inv.customer
      amount: event.paid_amount
      method: event.method
      gateway: "'xendit'"
      gateway_ref: event.gateway_ref
      reference: inv.name
      allocations: "min(event.paid_amount, inv.outstanding - drafts.map(p, p.allocated_total).sum()) > 0 ? [{'sales_invoice': inv.name, 'amount': min(event.paid_amount, inv.outstanding - drafts.map(p, p.allocated_total).sum())}] : []"
    as: pay
  # a payment made inside a flow does not run rangka-apps' claim flows (the kernel does not nest flows), so the flow
  # claims the id itself, as payment_gateway_claim_created does: a claim left by a deleted draft is taken over, else
  # a new claim, which the unique index refuses for the second of two racing callbacks
  - {find: gateway_ref_claim, where: gateway_ref == event.gateway_ref, as: held}
  - update: held
    if: done == null && inv != null && held != null
    set: {payment: pay.name}
  - create: gateway_ref_claim
    if: done == null && inv != null && held == null
    set: {gateway_ref: event.gateway_ref, payment: pay.name}
```

## How a connector is used

A connector is installed per site; its secrets come from `${ENV}` values, never from the file. Apps use it from
flows — the CRM app turns an incoming WhatsApp message into a lead and an activity:

```yaml
# rangka-apps/apps/crm/flows/whatsapp_to_lead.rk (excerpt)
on: whatsapp_business.message_received
steps:
  - {find: lead, where: phone == event.from, as: lead}
  - if: lead == null
    create: lead
    set: {lead_name: event.profile_name, phone: event.from, source: "'whatsapp'"}
idempotency: event.message_id
```

Connectors are different from **drivers** in `system.rk`: SMTP, FCM push and SMS deliver the system's own
notifications, while connectors integrate the business with outside services. They are also different from
`omilo-pieces`, the AI-facing tools of Omilo; Omilo reaches outside systems through Rangka over MCP.

## Writing a connector

Create `<name>/connector.rk` with the schema line
`# yaml-language-server: $schema=https://rangka-os.github.io/rangka-os/schema/0.4/connector.json` and declare `auth` (fields marked
`secret: true`), `triggers` (`via: webhook` with verification, or `via: poll` naming the `action` to send, with
`input`, `cursor` and optionally `each` for one call per outlet; `for_each` and `emit` mapping) and `actions`
(request, response mapping, `retry`). Validate it together with the apps:

```bash
python3 ../rangka-os/tools/rangka_validate.py ../rangka-apps/apps --site ../rangka-apps/sites/kopi-senja --connectors .
```

Schema: [rangka-os/schema/0.4/connector.json](https://github.com/rangka-os/rangka-os/blob/main/schema/0.4/connector.json).

## Contributing

Each new connector needs the vendor's official API docs linked in `docs:`, every secret marked `secret: true`, an
idempotency key for each trigger, and a flow example in rangka-apps that uses it.

## Licence

Elastic License 2.0 (ELv2), see `LICENSE`. Copyright 2026 Saga Tekno Studio. The code is public and free to
self-host, change and use in your own business. You may not offer it to others as a hosted or managed service;
partners who want to host Rangka for clients need a partner agreement with Saga. Rangka Cloud is the only
closed part. Contributions need a contributor agreement (to be published).
