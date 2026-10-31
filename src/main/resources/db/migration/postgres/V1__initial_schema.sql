-- 1. Core Business Table
CREATE TABLE orders
(
    id           UUID PRIMARY KEY,
    customer_id  VARCHAR(255)             NOT NULL,
    total_amount NUMERIC(12, 2)           NOT NULL,
    status       VARCHAR(50)              NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version      BIGINT                   NOT NULL DEFAULT 0
);

-- 2. Transactional Outbox Table
CREATE TABLE outbox
(
    id            UUID PRIMARY KEY,
    aggregatetype VARCHAR(255)             NOT NULL,
    aggregateid   VARCHAR(255)             NOT NULL,
    aggregate_key VARCHAR(255)             NOT NULL,
    type          VARCHAR(255)             NOT NULL,
    payload       TEXT                     NOT NULL,
    target_topic  VARCHAR(255)             NOT NULL,
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_outbox_created_at ON outbox (created_at);

-- 3. Status Timeline of each order, source of truth for order history repairs
CREATE TABLE order_status_changes
(
    id         UUID PRIMARY KEY,
    order_id   UUID                     NOT NULL REFERENCES orders (id),
    status     VARCHAR(50)              NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE INDEX idx_order_status_changes_order_id ON order_status_changes (order_id, created_at);
