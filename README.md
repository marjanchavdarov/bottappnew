# katalog-prices

Croatian grocery price data pipeline. Downloads daily price files published by
Croatian retailers (required by law since May 2025), cleans them, and stores
them in Supabase for use by the katalog.ai app.

---

## What it does

- Parses price CSV/XML files from Konzum, Spar, Lidl, Kaufland, Plodine, Tommy, Studenac
- Deduplicates products by EAN barcode into a single `master_products` table
- Tracks daily prices per store in `store_prices` (automatic price history)
- Feeds the barcode scanner and cross-store price comparison features in katalog.ai

---

## Project structure

```
katalog-prices/
├── ingest.py            # Main pipeline — parse files, upload to Supabase
├── test_connection.py   # Verify your Supabase setup before ingesting
├── create_tables.sql    # Run once in Supabase SQL Editor to create tables
├── requirements.txt
├── .env.example         # Copy to .env and fill in your keys
└── README.md
```

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/YOUR_USERNAME/katalog-prices.git
cd katalog-prices
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your Supabase credentials:
```
SUPABASE_URL=https://yourproject.supabase.co
SUPABASE_KEY=your_anon_public_key
SUPABASE_SERVICE_KEY=your_service_role_key
```

Find these in your Supabase dashboard under **Settings → API**.

### 3. Create tables in Supabase

Open your Supabase dashboard → **SQL Editor** → paste the contents of
`create_tables.sql` → click **Run**.

This creates:
- `master_products` — one row per EAN barcode (name, brand, image, category)
- `store_prices` — one row per barcode + store + date (price history)
- `today_prices` — view: latest price across all stores
- `cheapest_today` — view: cheapest store per product today

### 4. Test your connection

```bash
python test_connection.py
```

All checks should show ✅ before you run the ingestor.

---

## Usage

### Preview a file (no upload — safe to test)

```bash
python ingest.py --store konzum --file konzum_ivanicgrad.csv --preview-only
```

### Ingest a file into Supabase

```bash
python ingest.py --store konzum --file konzum_ivanicgrad.csv
python ingest.py --store studenac --file studenac_prices.xml
```

### Supported stores

| Store | Format | Notes |
|-------|--------|-------|
| konzum | CSV (`;` separated, cp1250 encoding) | One file per store location |
| spar | CSV | Similar format to Konzum |
| studenac | XML | Tag names may need adjusting |
| lidl | CSV | |
| kaufland | CSV | |
| plodine | CSV | |
| tommy | CSV | |

---

## Where to download price files

Croatian retailers are legally required to publish daily price lists:

| Store | URL |
|-------|-----|
| Konzum | konzum.hr/datoteke_cjenici |
| Spar | spar.hr/datoteke_cjenici/index.html |
| Lidl | tvrtka.lidl.hr/popis-mpc |
| Kaufland | kaufland.hr/popis-mpc |
| Plodine | plodine.hr/info-o-cijenama |
| Studenac | studenac.hr (XML files) |
| Aggregator | cijene.dev (open-source, all stores) |

---

## Supabase tables

### `master_products`
| Column | Type | Description |
|--------|------|-------------|
| barcode | text PK | EAN-13 barcode |
| name | text | Product name (Croatian) |
| brand | text | Brand name |
| category | text | Product category |
| unit | text | kg, L, kom, etc. |
| quantity | numeric | Net quantity |
| image_url | text | Product image (added later via Open Food Facts) |

### `store_prices`
| Column | Type | Description |
|--------|------|-------------|
| barcode | text FK | Links to master_products |
| store | text | konzum, lidl, spar, etc. |
| price_date | date | Date of this price record |
| current_price | numeric | What you pay today |
| regular_price | numeric | Normal (non-sale) price |
| sale_price | numeric | Discounted price (null if not on sale) |
| is_on_sale | boolean | True if currently on promotion |

---

## Roadmap

- [x] Konzum CSV parser
- [x] Studenac XML parser
- [ ] Auto-download all store files (crawler)
- [ ] Image hydration via Open Food Facts API
- [ ] Weekly cron job on Render
- [ ] `/api/barcode` endpoint in katalog.ai
- [ ] In-app barcode scanner (ZXing.js)

---

## Related

- **katalog.ai** — the main consumer PWA (botapp repo)
- **cijene.dev** — open-source Croatian price aggregator (backup data source)
- **Open Food Facts** — free product image database by EAN barcode
