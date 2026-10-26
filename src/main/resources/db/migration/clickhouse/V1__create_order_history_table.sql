CREATE TABLE IF NOT EXISTS order_history (
    order_id UUID,
    customer_id String,
    total_amount Decimal(18, 4),
    status String,
    created_at DateTime64(3),
    updated_at DateTime64(3),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(created_at)
ORDER BY (customer_id, order_id, updated_at)
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
