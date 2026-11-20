# rangka_whatsapp: WhatsApp from Rangka Cloud

A **host connector** (rangka-os contract 0063 §2 and §7): the business sends WhatsApp template messages from its own
number, which Rangka Cloud provides under Rangka's Meta setup. The owner signs up with nobody and enters no key: every
auth field is written by the host with a `secrets:host` token, and a kernel serves the connector only when whoever runs
it names it in `RANGKA_HOST_CONNECTORS`. A self-hosted owner uses [`whatsapp_business`](../whatsapp_business) with
their own Meta account instead.

**Status: spec, tested against a local mock only.** Rangka has no Meta account yet; no message can be sent. `hosts:`
is a placeholder under the reserved `.example` domain until Rangka Cloud has a public service address, so no bearer can
leave for a real host before then.

## How it works (option B: host relay)

```
flow ─ call rangka_whatsapp.send_template ─► kernel job ─► POST https://{api_host}/svc/whatsapp/v1/messages
                                                              │ Rangka Cloud: kill switch → service state → message
                                                              │ type → template approved → opt-out → daily cap →
                                                              │ number throttle → send once per idempotency_key
                                                              ▼
                                                             Meta
kernel /hooks/<org>/rangka_whatsapp/{message_status|message_received|opted_out} ◄── Rangka Cloud, signed per business
```

What the kernel holds works for this business only and is not Meta's:

| Field | What | Secret |
| --- | --- | --- |
| `api_host` | Rangka Cloud's service host | no |
| `service_token` | the business's relay bearer | yes |
| `webhook_key` | the key the host signs this business's events with | yes |

## Actions

`send_template {to, template, type, components, idempotency_key}` → `{message_id, status}`.

- `type` is the business's message type: `receipt`, `reminder`, `order_ready`, `appointment`, `order_reply`. Each can be switched off
  on its own.
- `components` is one body component with text parameters, in the template's order. The host rebuilds what goes to
  Meta from them; a header, a button or media is refused.
- `idempotency_key` is `<document>:<type>:<step>`, for example `STRK-KPI-2611-00142:receipt:1`. A key is sent once: the
  kernel never queues the same key twice, and the host answers a repeat with the first message (`duplicate: true`)
  without asking Meta.

`set_opt_out {phone}` → `{status: recorded | already}`. The business marked the customer "Tidak mau dikirimi WA". It
only ever adds to the host's list.

## Refusals

Every refusal is `{"error": {"code": "...", "field": "..."}}`. The connector retries **only** 429 and 5xx; every other
code is final and arrives at the kernel again as a `message_status` event with `status: failed` and the code in `error`,
so the document can say why nothing was sent.

| HTTP | Code | Meaning | Retried |
| --- | --- | --- | --- |
| 400 | `bad_request` (+ `field`) | malformed: `to`, `type`, `template`, `components`, `idempotency_key`, `body` | no |
| 401 | `unauthenticated` | no bearer, a wrong one, or a revoked one (switch-off) | no |
| 403 | `service_stopped` | Rangka stopped the service (kill switch), it is switched off or not provisioned yet | no |
| 403 | `service_paused` | held: a low quality rating, a re-categorised template, or this message type is off | no |
| 403 | `quota_exceeded` | the business's daily cap is used up | no |
| 409 | `template_not_approved` | no approved Utility version of this template for the business | no |
| 409 | `in_progress` | the same key is being sent right now | no |
| 409 | `delivery_uncertain` | an earlier send of this key got no answer from Meta: it may have arrived, so it is never sent again | no |
| 422 | `recipient_opted_out` | the customer asked this business to stop | no |
| 422 | `send_failed` | Meta refused the message for good (for example the number is not on WhatsApp) | no |
| 429 | `rate_limited` | try again after `Retry-After` | yes |
| 502 | `upstream_unavailable` | Meta is down or unreachable; nothing was sent | yes |
| 503 | `unavailable` | the host could not check (its store or the kill switch) | yes |

## Triggers

Every event is one JSON object, signed over the raw body: `X-Rangka-Signature: sha256=<hex HMAC-SHA256 with webhook_key>`.

| Trigger | Fields | When |
| --- | --- | --- |
| `message_status` | `message_id`, `idempotency_key`, `type`, `status` (`sent`, `delivered`, `read`, `failed`), `at`, `error` | Meta reported a status, or the host refused a send (`failed`, `message_id` empty, the code in `error`) |
| `message_received` | `message_id`, `from`, `text`, `kind`, `profile_name`, `at`, `lines`, `notes`, `order_signal` | a customer wrote to the number (it only sends in phase 1). The host reads a text as an order (deterministic, no catalogue): `lines` are `{qty, qty_given, unit, name, note, text}` with `name` lower-case ASCII words, `notes` the pick-up, delivery or time remarks, `order_signal` true when it reads as an order (an order word, a quantity, or a list of two or more). rangka-apps module `whatsapp_order` matches the lines against the business's items |
| `opted_out` | `phone`, `at`, `source` (`keyword`, `meta`, `staff`) | a customer replied `BERHENTI` or `STOP`, or Meta reported a block |

A status never carries the phone: the flow finds its document by `idempotency_key`.

## Templates

The canonical library is Rangka Cloud's (`internal/whatsapp/templates/library.yaml`); all are category Utility,
language `id`, and end with *Balas BERHENTI untuk berhenti menerima pesan.*

| Name | Type | Body (`{{n}}` = parameter) |
| --- | --- | --- |
| `struk_pembayaran` | `receipt` | Terima kasih, {{1}}. Struk {{2}} dari {{3}}: {{4}}. Total {{5}}, dibayar {{6}}. |
| `tagihan_terbit` | `reminder` | Halo {{1}}, berikut tagihan {{2}} no. {{3}} sebesar {{4}}, jatuh tempo {{5}}. Pembayaran ke {{6}}. Mohon tulis {{3}} di berita transfer. Terima kasih. |
| `pengingat_jatuh_tempo` | `reminder` | Halo {{1}}, kami mengingatkan faktur {{2}} sebesar {{3}} sudah lewat {{4}} hari dari jatuh tempo ({{5}}). Pembayaran ke {{6}}. Mohon tulis {{2}} di berita transfer. Abaikan pesan ini kalau sudah membayar. Terima kasih. |
| `terima_kasih_lunas` | `reminder` | Halo {{1}}, terima kasih. Pembayaran faktur {{2}} sudah kami terima dan faktur ini sudah lunas. |
| `pesanan_siap` | `order_ready` | Halo {{1}}, pesanan {{2}} di {{3}} sudah siap diambil. Sebutkan nomor pesanan saat mengambil. Terima kasih. |
| `pengingat_janji` | `appointment` | Halo {{1}}, kami mengingatkan {{2}} di {{3}} pada {{4}}. Kalau perlu mengubah jadwal, hubungi kami sebelum waktunya. Terima kasih. |
| `pesanan_diterima` | `order_reply` | Halo {{1}}, pesanan {{2}} di {{3}} sudah kami terima: {{4}}. Total {{5}}. Terima kasih. |

## Local mock

`mock/rangka_whatsapp_mock.py` stands in for the relay (dummy keys only, from `RANGKA_WHATSAPP_MOCK_TOKEN` and
`RANGKA_WHATSAPP_MOCK_WEBHOOK_KEY`; 127.0.0.1 only; stdlib). It answers both actions with every refusal above, sends a
key once, and posts signed events to `--hook-url`; `/simulate/*` routes change statuses, send inbound messages
(`BERHENTI` opts out), stop or pause the service, unapprove a template, queue a rate limit or an outage, and redeliver
the last event. `python3 tests/check_rangka_whatsapp_mock.py` (needs PyYAML) builds the requests from `connector.rk`
itself and checks every answer and event against it.

Rangka Cloud has its own stand-in with the real relay code behind a fake Meta: `rangka-cloud wa-dev`.
