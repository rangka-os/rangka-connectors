# Message templates

Templates sent outside the 24-hour window must be approved by Meta first (category **Utility**, language `id`).
Names are lowercase `snake_case`, as Meta requires; rangka-apps uses the same names in `invoice_reminder.template`
and in the print *Pesan tagihan WA*, so the manual reminder (sent from the owner's phone) and the automatic one say
the same thing. Utility templates carry no promotion; Meta charges per delivered message.

| Name | When | Body (`{{n}}` = parameter) |
| --- | --- | --- |
| `tagihan_terbit` | invoice submitted | Halo {{1}}, berikut tagihan {{2}} no. {{3}} sebesar {{4}}, jatuh tempo {{5}}. Pembayaran ke {{6}}. Mohon tulis {{3}} di berita transfer. Terima kasih. |
| `pengingat_jatuh_tempo` | due date passed, still owed | Halo {{1}}, kami mengingatkan faktur {{2}} sebesar {{3}} sudah lewat {{4}} hari dari jatuh tempo ({{5}}). Pembayaran ke {{6}}. Mohon tulis {{2}} di berita transfer. Abaikan pesan ini kalau sudah membayar. Terima kasih. |
| `terima_kasih_lunas` | invoice paid | Halo {{1}}, terima kasih. Pembayaran faktur {{2}} sudah kami terima dan faktur ini sudah lunas. |
| `struk_pembayaran` | a kasir sale is paid and the customer asked for the receipt on WhatsApp | Terima kasih, {{1}}. Struk {{2}} dari {{3}}: {{4}}. Total {{5}}, dibayar {{6}}. |
| `pesanan_siap` | a pick-up order is marked ready | Halo {{1}}, pesanan {{2}} di {{3}} sudah siap diambil. Sebutkan nomor pesanan saat mengambil. Terima kasih. |
| `pengingat_janji` | a booking starts tomorrow | Halo {{1}}, kami mengingatkan {{2}} di {{3}} pada {{4}}. Kalau perlu mengubah jadwal, hubungi kami sebelum waktunya. Terima kasih. |

Every template has the footer *Balas BERHENTI untuk berhenti menerima pesan.* (how to stop; the wording is to confirm
against Meta's template rules). A customer who replies BERHENTI or STOP must not be messaged again: with this
self-hosted connector that list is the owner's to keep (rangka-apps marks the customer "Tidak mau dikirimi WA" and the
flows skip them). With Rangka Cloud's [`rangka_whatsapp`](../rangka_whatsapp/README.md) the host also enforces it
before every send.

Parameters, in order:

- `tagihan_terbit`: customer name, business name, invoice number, amount (`Rp 1.234.000`), due date, bank account
  (`Bank Contoh 111 000 0001 a.n. PT Contoh`)
- `pengingat_jatuh_tempo`: customer name, invoice number, outstanding amount, days late, due date, bank account
- `terima_kasih_lunas`: customer name, invoice number
- `struk_pembayaran`: customer name, receipt number, business name, items, total, paid with
- `pesanan_siap`: customer name, order number, business name
- `pengingat_janji`: customer name, what (for example "janji potong rambut"), business name, when (date and time as one text)

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
