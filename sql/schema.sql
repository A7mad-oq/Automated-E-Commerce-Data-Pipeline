-- sql/schema.sql
-- Production database schema for the E-Commerce Data Pipeline.
--
-- Improvements over the original:
--   • Primary key and foreign-key constraints enforced
--   • Indexes on high-cardinality filter/join columns
--   • CHECK constraints to guard data quality at the DB layer
--   • Audit columns (created_at / updated_at) on every table
--   • Separate schema (ecommerce) to avoid polluting public
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS ecommerce;

-- ── orders ────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id                        VARCHAR(50)  NOT NULL,
    customer_id                     VARCHAR(50)  NOT NULL,
    order_status                    VARCHAR(20)  NOT NULL
        CHECK (order_status IN (
            'created','approved','processing',
            'shipped','delivered','unavailable','canceled','invoiced'
        )),
    order_purchase_timestamp        TIMESTAMP    NOT NULL,
    order_delivered_customer_date   TIMESTAMP,

    -- audit
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON ecommerce.orders (customer_id);

CREATE INDEX IF NOT EXISTS idx_orders_purchase_date
    ON ecommerce.orders (DATE(order_purchase_timestamp));

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON ecommerce.orders (order_status);

-- ── order_items ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    id              BIGSERIAL    NOT NULL,
    order_id        VARCHAR(50)  NOT NULL,
    product_id      VARCHAR(50)  NOT NULL,
    price           DECIMAL(10, 2) NOT NULL
        CHECK (price >= 0),
    freight_value   DECIMAL(10, 2) NOT NULL DEFAULT 0.00
        CHECK (freight_value >= 0),

    -- audit
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_order_items PRIMARY KEY (id),
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES ecommerce.orders (order_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON ecommerce.order_items (order_id);

CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON ecommerce.order_items (product_id);

-- ── Trigger: keep updated_at current on orders ────────────────────────────────
CREATE OR REPLACE FUNCTION ecommerce.set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_orders_updated_at'
    ) THEN
        CREATE TRIGGER trg_orders_updated_at
        BEFORE UPDATE ON ecommerce.orders
        FOR EACH ROW EXECUTE FUNCTION ecommerce.set_updated_at();
    END IF;
END;
$$;

-- ── Convenience view: daily revenue summary ───────────────────────────────────
CREATE OR REPLACE VIEW ecommerce.v_daily_revenue AS
SELECT
    DATE(o.order_purchase_timestamp)   AS report_date,
    COUNT(DISTINCT o.order_id)         AS order_count,
    ROUND(SUM(i.price)::NUMERIC, 2)   AS daily_revenue,
    ROUND(AVG(i.price)::NUMERIC, 2)   AS avg_order_value
FROM ecommerce.orders  o
JOIN ecommerce.order_items i USING (order_id)
WHERE o.order_status NOT IN ('canceled', 'unavailable')
GROUP BY DATE(o.order_purchase_timestamp)
ORDER BY report_date;
