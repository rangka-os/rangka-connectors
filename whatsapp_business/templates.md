# Message templates for payment collection

Templates sent outside the 24-hour window must be approved by Meta first (category **Utility**, language `id`).
Names are lowercase `snake_case`, as Meta requires; rangka-apps uses the same names in `invoice_reminder.template`
and in the print *Pesan tagihan WA*, so the manual reminder (sent from the owner's phone) and the automatic one say
the same thing. Utility templates carry no promotion; Meta charges per delivered message.

| Name | When | Body (`{{n}}` = parameter) |
| --- | --- | --- |
| `tagihan_terbit` | invoice submitted | Halo {{1}}, berikut tagihan {{2}} no. {{3}} sebesar {{4}}, jatuh tempo {{5}}. Pembayaran ke {{6}}. Mohon tulis {{3}} di berita transfer. Terima kasih. |
| `pengingat_jatuh_tempo` | due date passed, still owed | Halo {{1}}, kami mengingatkan faktur {{2}} sebesar {{3}} sudah lewat {{4}} hari dari jatuh tempo ({{5}}). Pembayaran ke {{6}}. Mohon tulis {{2}} di berita transfer. Abaikan pesan ini kalau sudah membayar. Terima kasih. |
| `terima_kasih_lunas` | invoice paid | Halo {{1}}, terima kasih. Pembayaran faktur {{2}} sudah kami terima dan faktur ini sudah lunas. |

Parameters, in order:

- `tagihan_terbit`: customer name, business name, invoice number, amount (`Rp 1.234.000`), due date, bank account
  (`Bank Contoh 111 000 0001 a.n. PT Contoh`)
- `pengingat_jatuh_tempo`: customer name, invoice number, outstanding amount, days late, due date, bank account
- `terima_kasih_lunas`: customer name, invoice number

`send_template` takes them as `components`:

```yaml
- call: whatsapp_business.send_template
  with:
    to: customer.phone
    template: "'pengingat_jatuh_tempo'"
    components: "[{'type': 'body', 'parameters': [{'type': 'text', 'text': customer.customer_name}, ...]}]"
```

Onboarding a business's own number goes through Meta's Embedded Signup as a Tech Provider; the older Embedded
Signup flow stopped on 15 October 2026.
