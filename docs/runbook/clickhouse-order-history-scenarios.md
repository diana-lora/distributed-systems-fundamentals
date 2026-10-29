# Runbook: ClickHouse Order History Scenarios

## Purpose

This runbook walks through the five order history scenarios of
[ADR 0003](../adr/0003-clickhouse-for-order-history.md) on a local stack. Each scenario lists the commands to run and
the result to expect. The design note [ClickHouse Order History: Scenarios Explained](../design/clickhouse-order-history-scenarios.md)
explains why the results look the way they do.

## Prerequisites

1. Build the application and start the stack from the `infra` folder:
   ```bash
   ./gradlew clean bootJar
   cd infra
   docker compose up --build -d
   ```
2. If the stack ran before, remove its volumes first with `docker compose down -v`. The demo orders are inserted only
   once per database, and an existing database may hold an older schema.
3. Wait until the application is healthy: `curl -s http://localhost:8080/actuator/health` returns `"status":"UP"`.
4. Wait until the demo orders reach the order history (usually a few seconds after the connector is running):
   ```bash
   curl -s "http://localhost:8080/ordersHistory"
   ```
   The response must contain four orders.

When the application runs in IntelliJ instead of Docker, set `APP_DEMO_DATA_ENABLED=true` in the run configuration to
insert the demo orders.

## Demo Data

The application inserts the following orders into PostgreSQL on the first start. Debezium and the consumer copy them into
ClickHouse, so every history version is a real Kafka offset. All times are UTC.

| Order ID                               | Customer         | Timeline                                                       | Used in        |
|:---------------------------------------|:-----------------|:---------------------------------------------------------------|:---------------|
| `10000000-0000-4000-8000-000000000001` | `customer-alice` | `CREATED` 2026-01-15 17:00, `PROCESSING` 17:05, `COMPLETED` 17:10 | Scenario 2, 4 |
| `10000000-0000-4000-8000-000000000002` | `customer-alice` | `CREATED` 2026-01-15 18:00 (wrong amount `99.00`), `PROCESSING` 18:05 | Scenario 3 |
| `10000000-0000-4000-8000-000000000003` | `customer-bob`   | `CREATED` 2026-01-20 09:00, `PROCESSING` 09:05                  | Scenario 5     |
| `10000000-0000-4000-8000-000000000004` | `customer-bob`   | `CREATED` 2026-02-03 10:00                                     | Scenario 4     |

## Helper Commands

Run a ClickHouse query:

```bash
ch() {
  docker exec clickhouse-server clickhouse-client --user ch_user --password ch_password \
    --database orders_analytics --query "$1"
}
```

Show every stored version of an order, including rows that a time travel query hides:

```bash
rows() {
  ch "SELECT version, created_at, updated_at, status, total_amount FROM order_history
      WHERE order_id = '$1' ORDER BY updated_at, version FORMAT PrettyCompact"
}
```

Read a time travel snapshot of an order:

```bash
snapshot() {
  curl -s "http://localhost:8080/ordersHistory?orderId=$1&targetTime=$2"; echo
}
```

## Scenario 1: Order Creation

**Goal:** A new order appears in the order history as its first version.

1. Create an order:
   ```bash
   curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"customerId": "customer-carol", "totalAmount": 12.50}'; echo
   ```
2. Copy the `id` from the response and show its stored rows: `rows <id>`.

**Expected result:** One row with status `CREATED`, `created_at` equal to `updated_at`, and a `version` equal to the Kafka
offset of the event.

## Scenario 2: Order Status Update

**Goal:** Every status change is appended as a new row, and earlier states remain readable.

1. Show the stored rows of the first demo order:
   ```bash
   rows 10000000-0000-4000-8000-000000000001
   ```
2. Read snapshots before, between and after the status changes:
   ```bash
   snapshot 10000000-0000-4000-8000-000000000001 2026-01-15T16:59:00Z
   snapshot 10000000-0000-4000-8000-000000000001 2026-01-15T17:02:00Z
   snapshot 10000000-0000-4000-8000-000000000001 2026-01-15T17:06:00Z
   snapshot 10000000-0000-4000-8000-000000000001 2026-01-15T17:11:00Z
   ```
3. Force a background merge and repeat the `17:02` snapshot:
   ```bash
   ch "OPTIMIZE TABLE order_history FINAL"
   snapshot 10000000-0000-4000-8000-000000000001 2026-01-15T17:02:00Z
   ```

**Expected result:** Three rows with increasing versions. The snapshots return `[]`, `CREATED`, `PROCESSING` and
`COMPLETED`. After the merge, the `17:02` snapshot still returns `CREATED`, because every state has its own `updated_at`
and therefore its own sorting key.

## Scenario 3: Retroactive Total Amount Correction

**Goal:** A wrong amount is corrected in every past snapshot, while the status timeline stays unchanged.

1. Confirm that both versions of the second demo order hold the wrong amount `99.00`:
   ```bash
   rows 10000000-0000-4000-8000-000000000002
   ```
2. Correct the amount:
   ```bash
   curl -s -X PUT http://localhost:8080/orders/totalAmount -H 'Content-Type: application/json' \
     -d '{"orderId": "10000000-0000-4000-8000-000000000002", "customerId": "customer-alice", "totalAmount": 9.90}'
   ```
3. Show the rows and the snapshots before and after the status change:
   ```bash
   rows 10000000-0000-4000-8000-000000000002
   snapshot 10000000-0000-4000-8000-000000000002 2026-01-15T18:02:00Z
   snapshot 10000000-0000-4000-8000-000000000002 2026-01-15T18:06:00Z
   ```

**Expected result:** Still two rows with unchanged versions and timestamps, both with `total_amount = 9.90`. The `18:02`
snapshot returns `CREATED` with `9.90`, and the `18:06` snapshot returns `PROCESSING` with `9.90`.

## Scenario 4: Historical Point-in-Time Reconstruction (Time Travel)

**Goal:** A snapshot returns every order exactly as it was at the target time, one row per order.

1. Read the orders of `customer-alice` at `17:30`:
   ```bash
   curl -s "http://localhost:8080/ordersHistory?customerId=customer-alice&targetTime=2026-01-15T17:30:00Z"; echo
   ```
2. Read all orders at the end of January and at the end of February:
   ```bash
   curl -s "http://localhost:8080/ordersHistory?targetTime=2026-01-31T23:59:00Z"; echo
   curl -s "http://localhost:8080/ordersHistory?targetTime=2026-02-28T23:59:00Z"; echo
   ```

**Expected result:**

* At `17:30`, `customer-alice` has only order `...0001` with status `COMPLETED`. Order `...0002` did not exist yet.
* At the end of January, three demo orders are returned. The February order `...0004` is absent; ClickHouse skips its
  monthly partition because of the `created_at` filter.
* At the end of February, all four demo orders are returned.

## Scenario 5: History Rebuild (Discrepancy Troubleshooting)

**Goal:** Repair an order whose history no longer matches PostgreSQL, including rows that never happened.

1. Simulate drift: store a `COMPLETED` state at `09:06` that never happened in PostgreSQL. It copies the newest version
   of the order, so it wins every snapshot after `09:06`:
   ```bash
   ch "INSERT INTO order_history
       SELECT order_id, customer_id, total_amount, 'COMPLETED', created_at, updated_at + INTERVAL 1 MINUTE, version
       FROM order_history WHERE order_id = '10000000-0000-4000-8000-000000000003'
       ORDER BY version DESC, updated_at DESC LIMIT 1"
   ```
2. Confirm the discrepancy. PostgreSQL still reports `PROCESSING`, while the history reports `COMPLETED`:
   ```bash
   rows 10000000-0000-4000-8000-000000000003
   snapshot 10000000-0000-4000-8000-000000000003 2026-01-20T09:10:00Z
   curl -s http://localhost:8080/orders | grep -o '"id":"10000000-0000-4000-8000-000000000003"[^}]*'
   ```
3. Trigger the history rebuild:
   ```bash
   curl -s -o /dev/null -w '%{http_code}\n' -X POST \
     http://localhost:8080/orders/10000000-0000-4000-8000-000000000003/history/repair
   ```
4. Verify the repaired history:
   ```bash
   rows 10000000-0000-4000-8000-000000000003
   snapshot 10000000-0000-4000-8000-000000000003 2026-01-20T09:02:00Z
   snapshot 10000000-0000-4000-8000-000000000003 2026-01-20T09:10:00Z
   ```

**Expected result:**

* Step 3 returns `202`; the rebuild runs asynchronously once the repair event is consumed.
* After the rebuild, exactly two rows remain: `CREATED` at `09:00` and `PROCESSING` at `09:05`. Both share one new
  version, higher than every earlier version. The `COMPLETED` row and the older versions are deleted.
* The `09:02` snapshot returns `CREATED`, and the `09:10` snapshot returns `PROCESSING` with `updated_at` `09:05`.

**If the rows are not repaired:** Check the application logs (`docker logs outbox-spring-app`) for the repair message
of the order, and the order events topic in Kafka UI (`http://localhost:8082`) for an `ORDER_REPAIR` message. If the
consumer failed, follow [Kafka Poison Pill Inspection and Re-drive](kafka-poison-pill-handling.md).

## Reset

Remove all containers and volumes to start again with only the demo orders:

```bash
docker compose down -v
```
