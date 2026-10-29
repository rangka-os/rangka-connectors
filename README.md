# rangka-connectors

Plug-and-play business integrations for Rangka OS, as declarative `connector.rk` files: auth, triggers (webhooks,
polling), actions, field mapping, retries and idempotency. Deterministic: no LLM in the path. (AI-facing tools for
Omilo helpers live in omilo-pieces; Omilo reaches outside systems through Rangka over MCP.)

| Connector | Status |
| --- | --- |
| `whatsapp_business` (Meta Cloud API) | Spec: webhook trigger with X-Hub-Signature-256 check, `send_template` action |

Planned: QRIS (Xendit, Midtrans), e-Faktur / Coretax, Tokopedia, Shopee, TikTok Shop, bank statements, GoFood/GrabFood,
Accurate/Jurnal import.

Validate: `python3 ../rangka-os/tools/rangka_validate.py ../rangka-apps/apps --connectors .`
Schema: `rangka-os/schema/0.4/connector.json`. The kernel has no connector runtime yet (v0 records `call` steps in the audit log).
