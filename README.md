# Rangka Connectors

**Plug-and-play integrations for [Rangka OS](https://github.com/rangka-os/rangka-os)**, each declared in one
`connector.rk` file: how to authenticate, which events come in (webhooks, polling), which actions go out, how fields
map, and how retries and duplicates are handled. Connectors are deterministic: no LLM sits in the data path.

> **Status: v0 (October 2026).** The format and its schema are done, with WhatsApp Business as the first connector.
> The kernel does not run connectors yet: in v0, a flow's `call` step is recorded in the audit log only.

Part of the Rangka family: [rangka-os](https://github.com/rangka-os/rangka-os) ·
[rangka-apps](https://github.com/rangka-os/rangka-apps) · **rangka-connectors** ·
[rangka-ai](https://github.com/rangka-os/rangka-ai)

## Connectors

| Connector | Service | Triggers | Actions | Status |
| --- | --- | --- | --- | --- |
| [`whatsapp_business`](whatsapp_business/connector.rk) | Meta WhatsApp Cloud API | `message_received`, `message_status` (webhook, `X-Hub-Signature-256` checked) | `send_template` (with retries); collection templates in [templates.md](whatsapp_business/templates.md) | Spec |
| [`xendit`](xendit/connector.rk) | Xendit (VA, QRIS, payment links; xenPlatform sub-accounts) | `va_paid`, `qr_paid`, `invoice_paid` (webhook, `x-callback-token` checked; every callback, `paid` marks a completed payment) | `create_fixed_va`, `create_qr_code`, `create_invoice`, `get_invoice` (with retries, idempotent on the invoice number and amount) | Spec |

Planned, in rough order: QRIS (Midtrans), bank statements, e-Faktur / Coretax, Tokopedia, Shopee, TikTok Shop,
GoFood / GrabFood, Accurate / Jurnal import.

A payment connector ends in a `payment` whose `gateway_ref` is held by one live payment at a time in rangka-apps
(`gateway_ref_claim`): one invoice is never paid twice by a repeated callback, and a cancelled payment gives its id
back. The flow below is for `xendit.va_paid`; the same steps serve `qr_paid` and `invoice_paid`, whose callbacks also
arrive for non-payments (an `EXPIRED` link, a QR that did not succeed), which is what `if: event.paid` filters out.

```yaml
# a site flow until ports land (contract 0002 §B); modules will call payment.* ports instead of xendit.*
on: xendit.va_paid
if: event.paid            # the trigger fires for every callback; only a completed payment records money
idempotency: event.gateway_ref
steps:
  # a live payment already holds this gateway id: the callback came again, do nothing
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
  # claims the id itself; two callbacks racing both pass `done` and the unique claim refuses the second
  - create: gateway_ref_claim
    if: done == null && inv != null
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
`secret: true`), `triggers` (`via: webhook` or `poll`, verification, `emit` mapping) and `actions` (request,
response mapping, `retry`). Validate it together with the apps:

```bash
python3 ../rangka-os/tools/rangka_validate.py ../rangka-apps/apps --site ../rangka-apps/sites/kopi-senja --connectors .
```

Schema: [rangka-os/schema/0.4/connector.json](https://github.com/rangka-os/rangka-os/blob/main/schema/0.4/connector.json).

## Contributing

Each new connector needs the vendor's official API docs linked in `docs:`, every secret marked `secret: true`, an
idempotency key for each trigger, and a flow example in rangka-apps that uses it.

## Licence

Not chosen yet; no `LICENSE` file is committed, so all rights are reserved until one is.
