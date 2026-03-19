-- ============================================================
-- katalog.ai — New tables for government price data pipeline
-- Run this in your Supabase SQL Editor
-- ============================================================


-- 1. master_products
--    One row per EAN barcode. Shared across all stores.
--    Product name, brand, image, category live here.
-- ============================================================
CREATE TABLE IF NOT EXISTS master_products (
    barcode         text PRIMARY KEY,          -- EAN-13 barcode (unique key)
    name            text NOT NULL,             -- Product name (from store file)
    brand           text,                      -- Brand name
    category        text,                      -- Category (Croatian)
    unit            text,                      -- kg, L, kom, etc.
    quantity        numeric,                   -- Net quantity (numeric)
    image_url       text,                      -- Product image (from Open Food Facts or store CDN)
    created_at      timestamptz DEFAULT now(),
    updated_at      timestamptz DEFAULT now()
);

-- Index for fast name search
CREATE INDEX IF NOT EXISTS idx_master_products_name
    ON master_products USING gin(to_tsvector('simple', name));

-- Index for category filtering
CREATE INDEX IF NOT EXISTS idx_master_products_category
    ON master_products (category);


-- 2. store_prices
--    One row per barcode + store + date.
--    Tracks price history automatically.
-- ============================================================
CREATE TABLE IF NOT EXISTS store_prices (
    id              uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    barcode         text NOT NULL REFERENCES master_products(barcode) ON DELETE CASCADE,
    store           text NOT NULL,             -- konzum, lidl, spar, etc.
    price_date      date NOT NULL DEFAULT CURRENT_DATE,
    current_price   numeric,                   -- What you pay today
    regular_price   numeric,                   -- Normal (non-sale) price
    sale_price      numeric,                   -- Discounted price (NULL if not on sale)
    is_on_sale      boolean DEFAULT false,
    created_at      timestamptz DEFAULT now(),

    -- Only one price record per barcode+store per day
    UNIQUE (barcode, store, price_date)
);

-- Index for fast store lookups
CREATE INDEX IF NOT EXISTS idx_store_prices_store
    ON store_prices (store, price_date);

-- Index for barcode lookups (scanner feature)
CREATE INDEX IF NOT EXISTS idx_store_prices_barcode
    ON store_prices (barcode, price_date);


-- 3. Row Level Security — allow anon reads and writes
--    (same pattern as your existing tables)
-- ============================================================
ALTER TABLE master_products ENABLE ROW LEVEL SECURITY;
ALTER TABLE store_prices    ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow reads master_products"
    ON master_products FOR SELECT TO anon, authenticated USING (true);

CREATE POLICY "Allow writes master_products"
    ON master_products FOR INSERT TO anon, authenticated WITH CHECK (true);

CREATE POLICY "Allow updates master_products"
    ON master_products FOR UPDATE TO anon, authenticated USING (true);

CREATE POLICY "Allow reads store_prices"
    ON store_prices FOR SELECT TO anon, authenticated USING (true);

CREATE POLICY "Allow writes store_prices"
    ON store_prices FOR INSERT TO anon, authenticated WITH CHECK (true);


-- 4. Useful views for the app
-- ============================================================

-- today_prices: latest price per barcode across all stores (for scanner feature)
CREATE OR REPLACE VIEW today_prices AS
SELECT
    mp.barcode,
    mp.name,
    mp.brand,
    mp.image_url,
    mp.category,
    sp.store,
    sp.current_price,
    sp.regular_price,
    sp.sale_price,
    sp.is_on_sale
FROM master_products mp
JOIN store_prices sp ON sp.barcode = mp.barcode
WHERE sp.price_date = CURRENT_DATE;

-- cheapest_today: for each barcode, which store is cheapest right now
CREATE OR REPLACE VIEW cheapest_today AS
SELECT DISTINCT ON (barcode)
    barcode,
    store,
    current_price
FROM store_prices
WHERE price_date = CURRENT_DATE
  AND current_price IS NOT NULL
ORDER BY barcode, current_price ASC;
