-- Demo orders for the scenario runbook (docs/runbook/clickhouse-order-history-scenarios.md).
-- Only PostgreSQL is seeded: Debezium publishes the outbox rows and the consumer writes the order history, so every
-- history version is a real Kafka offset. Outbox rows are inserted in chronological order per customer, the order in
-- which Debezium publishes them.
--
-- | Order | Customer       | Timeline (UTC)                                        | Scenario                        |
-- |:------|:---------------|:------------------------------------------------------|:--------------------------------|
-- | ...01 | customer-alice | CREATED 2026-01-15 17:00, PROCESSING 17:05, COMPLETED 17:10 | 2 Status update, 4 Time travel |
-- | ...02 | customer-alice | CREATED 2026-01-15 18:00 (wrong amount 99.00), PROCESSING 18:05 | 3 Amount correction       |
-- | ...03 | customer-bob   | CREATED 2026-01-20 09:00, PROCESSING 09:05             | 5 History rebuild               |
-- | ...04 | customer-bob   | CREATED 2026-02-03 10:00                              | 4 Time travel (other partition) |

-- 1. Current state of each order (version = number of updates, as optimistic locking counts them)
INSERT INTO orders (id, customer_id, total_amount, status, created_at, version)
VALUES ('10000000-0000-4000-8000-000000000001', 'customer-alice', 49.90, 'COMPLETED', '2026-01-15T17:00:00Z', 2),
       ('10000000-0000-4000-8000-000000000002', 'customer-alice', 99.00, 'PROCESSING', '2026-01-15T18:00:00Z', 1),
       ('10000000-0000-4000-8000-000000000003', 'customer-bob', 25.00, 'PROCESSING', '2026-01-20T09:00:00Z', 1),
       ('10000000-0000-4000-8000-000000000004', 'customer-bob', 15.00, 'CREATED', '2026-02-03T10:00:00Z', 0);

-- 2. Status timeline, source of truth for history rebuilds
INSERT INTO order_status_changes (id, order_id, status, created_at)
VALUES ('20000000-0000-4000-8000-000000000001', '10000000-0000-4000-8000-000000000001', 'CREATED', '2026-01-15T17:00:00Z'),
       ('20000000-0000-4000-8000-000000000002', '10000000-0000-4000-8000-000000000001', 'PROCESSING', '2026-01-15T17:05:00Z'),
       ('20000000-0000-4000-8000-000000000003', '10000000-0000-4000-8000-000000000001', 'COMPLETED', '2026-01-15T17:10:00Z'),
       ('20000000-0000-4000-8000-000000000004', '10000000-0000-4000-8000-000000000002', 'CREATED', '2026-01-15T18:00:00Z'),
       ('20000000-0000-4000-8000-000000000005', '10000000-0000-4000-8000-000000000002', 'PROCESSING', '2026-01-15T18:05:00Z'),
       ('20000000-0000-4000-8000-000000000006', '10000000-0000-4000-8000-000000000003', 'CREATED', '2026-01-20T09:00:00Z'),
       ('20000000-0000-4000-8000-000000000007', '10000000-0000-4000-8000-000000000003', 'PROCESSING', '2026-01-20T09:05:00Z'),
       ('20000000-0000-4000-8000-000000000008', '10000000-0000-4000-8000-000000000004', 'CREATED', '2026-02-03T10:00:00Z');

-- 3. Outbox events, keyed by customer so that all events of an order share one Kafka partition
INSERT INTO outbox (id, aggregatetype, aggregateid, aggregate_key, type, payload, target_topic, created_at)
VALUES ('30000000-0000-4000-8000-000000000001', 'Order', '10000000-0000-4000-8000-000000000001', 'customer-alice',
        'ORDER_CREATED',
        '{"type":"ORDER_CREATED","orderId":"10000000-0000-4000-8000-000000000001","customerId":"customer-alice","totalAmount":49.90,"status":"CREATED","createdAt":"2026-01-15T17:00:00Z","updatedAt":"2026-01-15T17:00:00Z"}',
        'order-events', '2026-01-15T17:00:00Z'),
       ('30000000-0000-4000-8000-000000000002', 'Order', '10000000-0000-4000-8000-000000000001', 'customer-alice',
        'ORDER_STATUS_UPDATED',
        '{"type":"ORDER_STATUS_UPDATED","orderId":"10000000-0000-4000-8000-000000000001","customerId":"customer-alice","totalAmount":49.90,"newStatus":"PROCESSING","createdAt":"2026-01-15T17:00:00Z","updatedAt":"2026-01-15T17:05:00Z"}',
        'order-events', '2026-01-15T17:05:00Z'),
       ('30000000-0000-4000-8000-000000000003', 'Order', '10000000-0000-4000-8000-000000000001', 'customer-alice',
        'ORDER_STATUS_UPDATED',
        '{"type":"ORDER_STATUS_UPDATED","orderId":"10000000-0000-4000-8000-000000000001","customerId":"customer-alice","totalAmount":49.90,"newStatus":"COMPLETED","createdAt":"2026-01-15T17:00:00Z","updatedAt":"2026-01-15T17:10:00Z"}',
        'order-events', '2026-01-15T17:10:00Z'),
       ('30000000-0000-4000-8000-000000000004', 'Order', '10000000-0000-4000-8000-000000000002', 'customer-alice',
        'ORDER_CREATED',
        '{"type":"ORDER_CREATED","orderId":"10000000-0000-4000-8000-000000000002","customerId":"customer-alice","totalAmount":99.00,"status":"CREATED","createdAt":"2026-01-15T18:00:00Z","updatedAt":"2026-01-15T18:00:00Z"}',
        'order-events', '2026-01-15T18:00:00Z'),
       ('30000000-0000-4000-8000-000000000005', 'Order', '10000000-0000-4000-8000-000000000002', 'customer-alice',
        'ORDER_STATUS_UPDATED',
        '{"type":"ORDER_STATUS_UPDATED","orderId":"10000000-0000-4000-8000-000000000002","customerId":"customer-alice","totalAmount":99.00,"newStatus":"PROCESSING","createdAt":"2026-01-15T18:00:00Z","updatedAt":"2026-01-15T18:05:00Z"}',
        'order-events', '2026-01-15T18:05:00Z'),
       ('30000000-0000-4000-8000-000000000006', 'Order', '10000000-0000-4000-8000-000000000003', 'customer-bob',
        'ORDER_CREATED',
        '{"type":"ORDER_CREATED","orderId":"10000000-0000-4000-8000-000000000003","customerId":"customer-bob","totalAmount":25.00,"status":"CREATED","createdAt":"2026-01-20T09:00:00Z","updatedAt":"2026-01-20T09:00:00Z"}',
        'order-events', '2026-01-20T09:00:00Z'),
       ('30000000-0000-4000-8000-000000000007', 'Order', '10000000-0000-4000-8000-000000000003', 'customer-bob',
        'ORDER_STATUS_UPDATED',
        '{"type":"ORDER_STATUS_UPDATED","orderId":"10000000-0000-4000-8000-000000000003","customerId":"customer-bob","totalAmount":25.00,"newStatus":"PROCESSING","createdAt":"2026-01-20T09:00:00Z","updatedAt":"2026-01-20T09:05:00Z"}',
        'order-events', '2026-01-20T09:05:00Z'),
       ('30000000-0000-4000-8000-000000000008', 'Order', '10000000-0000-4000-8000-000000000004', 'customer-bob',
        'ORDER_CREATED',
        '{"type":"ORDER_CREATED","orderId":"10000000-0000-4000-8000-000000000004","customerId":"customer-bob","totalAmount":15.00,"status":"CREATED","createdAt":"2026-02-03T10:00:00Z","updatedAt":"2026-02-03T10:00:00Z"}',
        'order-events', '2026-02-03T10:00:00Z');
