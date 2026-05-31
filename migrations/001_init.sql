-- ============================================================
-- SagaForge AI – initial schema
-- Run against your Supabase project via the SQL editor or
-- `psql $DATABASE_URL -f migrations/001_init.sql`
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ------------------------------------------------------------
-- Saga state (owned by the saga-orchestrator)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sagas (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id        UUID NOT NULL UNIQUE,
    status          TEXT NOT NULL DEFAULT 'started',  -- see models.SagaStatus
    current_step    TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    failure_reason  TEXT
);

CREATE TABLE IF NOT EXISTS saga_steps (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saga_id      UUID NOT NULL REFERENCES sagas(id),
    step_name    TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending | in_progress | completed | failed | compensated
    started_at   TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_saga_steps_saga_id ON saga_steps(saga_id);

-- ------------------------------------------------------------
-- Transactional outbox (shared pattern – each service has rows here
-- identified by aggregate_type)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_events (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_id     TEXT NOT NULL,           -- e.g. order_id, payment_id
    aggregate_type   TEXT NOT NULL,           -- order | inventory | payment | fulfillment | saga | ai_insight
    event_type       TEXT NOT NULL,           -- e.g. order.created
    payload          JSONB NOT NULL,
    idempotency_key  TEXT NOT NULL UNIQUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at     TIMESTAMPTZ             -- NULL = not yet published
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox_events(created_at)
    WHERE published_at IS NULL;

-- ------------------------------------------------------------
-- Orders
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id  TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
    items        JSONB NOT NULL DEFAULT '[]',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Inventory reservations
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inventory_reservations (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id     UUID NOT NULL,
    sku          TEXT NOT NULL,
    quantity     INT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'reserved',   -- reserved | released
    reserved_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_inventory_order ON inventory_reservations(order_id);

-- Tracks stock levels
CREATE TABLE IF NOT EXISTS inventory_stock (
    sku           TEXT PRIMARY KEY,
    available_qty INT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Payments
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS payments (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id         UUID NOT NULL UNIQUE,
    amount           NUMERIC(12,2) NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',   -- pending | processed | failed | refunded
    idempotency_key  TEXT NOT NULL UNIQUE,
    processed_at     TIMESTAMPTZ,
    failed_at        TIMESTAMPTZ,
    refunded_at      TIMESTAMPTZ,
    failure_reason   TEXT
);

-- ------------------------------------------------------------
-- Fulfillments
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fulfillments (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id     UUID NOT NULL UNIQUE,
    status       TEXT NOT NULL DEFAULT 'pending',   -- pending | started | shipped | failed
    tracking_id  TEXT,
    started_at   TIMESTAMPTZ,
    shipped_at   TIMESTAMPTZ,
    failed_at    TIMESTAMPTZ
);

-- ------------------------------------------------------------
-- AI Insights
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ai_insights (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    saga_id        UUID NOT NULL REFERENCES sagas(id),
    order_id       UUID NOT NULL,
    trigger_event  TEXT NOT NULL,
    risk_score     NUMERIC(4,3),
    explanation    TEXT,
    suggestion     TEXT,
    generated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_insights_order ON ai_insights(order_id);

-- ------------------------------------------------------------
-- Seed some inventory stock for the demo
-- ------------------------------------------------------------
INSERT INTO inventory_stock (sku, available_qty) VALUES
    ('SKU-WIDGET-A', 1000),
    ('SKU-WIDGET-B', 500),
    ('SKU-GADGET-X', 250),
    ('SKU-GADGET-Y', 100)
ON CONFLICT (sku) DO NOTHING;
