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
