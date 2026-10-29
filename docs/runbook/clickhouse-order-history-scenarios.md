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
   docker compose --profile app up --build -d
   ```
2. If the stack ran before, remove its volumes first with `docker compose --profile app down -v`, so that the scenarios
   start from an empty database with the current schema.
3. Wait until the application is healthy: `curl -s http://localhost:8080/actuator/health` returns `"status":"UP"`.
4. Install `jq`; the commands below use it to read order IDs from the responses.

Run all commands of this runbook in the same shell session, because later steps use the variables of earlier steps.

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

Create an order and print its ID, change the status of an order, and record the current time:

```bash
create_order() {
  curl -s -X POST http://localhost:8080/orders -H 'Content-Type: application/json' \
    -d "{\"customerId\": \"$1\", \"totalAmount\": $2}" | jq -r .id
}

set_status() {
  curl -s -X PUT http://localhost:8080/orders/status -H 'Content-Type: application/json' \
    -d "{\"orderId\": \"$1\", \"customerId\": \"$2\", \"status\": \"$3\"}"
}

mark() {
  sleep 2; date -u +%Y-%m-%dT%H:%M:%SZ; sleep 2
}
```

`mark` pauses before and after it reads the clock, so every recorded time lies strictly between two order changes.

## Create the Scenario Data

Create the orders of Scenarios 2 to 5 through the REST API. The application stores the current time as `created_at`
and `updated_at`, so the variables `T_*` record the points in time that the snapshots use later.

```bash
T_START=$(mark)

# Scenarios 2 and 4: an order that runs through every status
STATUS_ORDER=$(create_order customer-alice 49.90)
T_STATUS_CREATED=$(mark)
set_status "$STATUS_ORDER" customer-alice PROCESSING
T_STATUS_PROCESSING=$(mark)
set_status "$STATUS_ORDER" customer-alice COMPLETED
T_STATUS_COMPLETED=$(mark)

# Scenario 3: an order created with the wrong amount 99.00
AMOUNT_ORDER=$(create_order customer-alice 99.00)
T_AMOUNT_CREATED=$(mark)
set_status "$AMOUNT_ORDER" customer-alice PROCESSING
T_AMOUNT_PROCESSING=$(mark)

# Scenario 5: an order whose history drifts later
REBUILD_ORDER=$(create_order customer-bob 25.00)
T_REBUILD_CREATED=$(mark)
set_status "$REBUILD_ORDER" customer-bob PROCESSING
T_REBUILD_PROCESSING=$(mark)
```

Wait until the orders reach the order history (usually a few seconds after the connector is running):

```bash
curl -s "http://localhost:8080/ordersHistory"; echo
```

The response must contain the three orders.

| Variable        | Customer         | Timeline                                         | Used in       |
|:----------------|:-----------------|:-------------------------------------------------|:--------------|
| `STATUS_ORDER`  | `customer-alice` | `CREATED`, `PROCESSING`, `COMPLETED`             | Scenario 2, 4 |
| `AMOUNT_ORDER`  | `customer-alice` | `CREATED` (wrong amount `99.00`), `PROCESSING`   | Scenario 3, 4 |
| `REBUILD_ORDER` | `customer-bob`   | `CREATED`, `PROCESSING`                          | Scenario 5    |

## Scenario 1: Order Creation

**Goal:** A new order appears in the order history as its first version.

1. Create an order:
   ```bash
   NEW_ORDER=$(create_order customer-carol 12.50); echo "$NEW_ORDER"
   ```
2. Show its stored rows: `rows $NEW_ORDER`.

**Expected result:** One row with status `CREATED`, `created_at` equal to `updated_at`, and a `version` equal to the Kafka
offset of the event.

## Scenario 2: Order Status Update

**Goal:** Every status change is appended as a new row, and earlier states remain readable.

1. Show the stored rows of the order:
   ```bash
   rows $STATUS_ORDER
   ```
2. Read snapshots before, between and after the status changes:
   ```bash
   snapshot $STATUS_ORDER $T_START
   snapshot $STATUS_ORDER $T_STATUS_CREATED
   snapshot $STATUS_ORDER $T_STATUS_PROCESSING
   snapshot $STATUS_ORDER $T_STATUS_COMPLETED
   ```
3. Force a background merge and repeat the `T_STATUS_CREATED` snapshot:
   ```bash
   ch "OPTIMIZE TABLE order_history FINAL"
   snapshot $STATUS_ORDER $T_STATUS_CREATED
   ```

**Expected result:** Three rows with increasing versions. The snapshots return `[]`, `CREATED`, `PROCESSING` and
`COMPLETED`. After the merge, the `T_STATUS_CREATED` snapshot still returns `CREATED`, because every state has its own
`updated_at` and therefore its own sorting key.

## Scenario 3: Retroactive Total Amount Correction

**Goal:** A wrong amount is corrected in every past snapshot, while the status timeline stays unchanged.

1. Confirm that both versions of the order hold the wrong amount `99.00`:
   ```bash
   rows $AMOUNT_ORDER
   ```
2. Correct the amount:
   ```bash
   curl -s -X PUT http://localhost:8080/orders/totalAmount -H 'Content-Type: application/json' \
     -d "{\"orderId\": \"$AMOUNT_ORDER\", \"customerId\": \"customer-alice\", \"totalAmount\": 9.90}"
   ```
3. Show the rows and the snapshots before and after the status change:
   ```bash
   rows $AMOUNT_ORDER
   snapshot $AMOUNT_ORDER $T_AMOUNT_CREATED
   snapshot $AMOUNT_ORDER $T_AMOUNT_PROCESSING
   ```

**Expected result:** Still two rows with unchanged versions and timestamps, both with `total_amount = 9.90`. The
`T_AMOUNT_CREATED` snapshot returns `CREATED` with `9.90`, and the `T_AMOUNT_PROCESSING` snapshot returns `PROCESSING`
with `9.90`.

## Scenario 4: Historical Point-in-Time Reconstruction (Time Travel)

**Goal:** A snapshot returns every order exactly as it was at the target time, one row per order.

1. Read the orders of `customer-alice` before the first order, after the first order was completed, and after the
   second order changed to `PROCESSING`:
   ```bash
   curl -s "http://localhost:8080/ordersHistory?customerId=customer-alice&targetTime=$T_START"; echo
   curl -s "http://localhost:8080/ordersHistory?customerId=customer-alice&targetTime=$T_STATUS_COMPLETED"; echo
   curl -s "http://localhost:8080/ordersHistory?customerId=customer-alice&targetTime=$T_AMOUNT_PROCESSING"; echo
   ```

**Expected result:**

* At `T_START`, the response is `[]`.
* At `T_STATUS_COMPLETED`, `customer-alice` has only `STATUS_ORDER` with status `COMPLETED`. `AMOUNT_ORDER` did not exist
  yet.
* At `T_AMOUNT_PROCESSING`, both orders are returned: `STATUS_ORDER` with `COMPLETED` and `AMOUNT_ORDER` with
  `PROCESSING`.

The `created_at <= targetTime` filter also lets ClickHouse skip whole monthly partitions of orders created after the
target time. The API always stores the current time as `created_at`, so this runbook cannot create orders in another
month; the design note explains the partition pruning.

## Scenario 5: History Rebuild (Discrepancy Troubleshooting)

**Goal:** Repair an order whose history no longer matches PostgreSQL, including rows that never happened.

1. Simulate drift: store a `COMPLETED` state one second after the newest version that never happened in PostgreSQL. It
   copies the newest version of the order, so it wins every later snapshot:
   ```bash
   ch "INSERT INTO order_history
       SELECT order_id, customer_id, total_amount, 'COMPLETED', created_at, updated_at + INTERVAL 1 SECOND, version
       FROM order_history WHERE order_id = '$REBUILD_ORDER'
       ORDER BY version DESC, updated_at DESC LIMIT 1"
   ```
2. Confirm the discrepancy. PostgreSQL still reports `PROCESSING`, while the history reports `COMPLETED`:
   ```bash
   rows $REBUILD_ORDER
   snapshot $REBUILD_ORDER $(mark)
   curl -s http://localhost:8080/orders | jq ".[] | select(.id == \"$REBUILD_ORDER\")"
   ```
3. Trigger the history rebuild:
   ```bash
   curl -s -o /dev/null -w '%{http_code}\n' -X POST \
     http://localhost:8080/orders/$REBUILD_ORDER/history/repair
   ```
4. Verify the repaired history:
   ```bash
   rows $REBUILD_ORDER
   snapshot $REBUILD_ORDER $T_REBUILD_CREATED
   snapshot $REBUILD_ORDER $(mark)
   ```

**Expected result:**

* Step 3 returns `202`; the rebuild runs asynchronously once the repair event is consumed.
* After the rebuild, exactly two rows remain: `CREATED` and `PROCESSING` with their original `updated_at`. Both share
  one new version, higher than every earlier version. The `COMPLETED` row and the older versions are deleted.
* The `T_REBUILD_CREATED` snapshot returns `CREATED`, and the current snapshot returns `PROCESSING` with the
  `updated_at` of the status change.

**If the rows are not repaired:** Check the application logs (`docker logs outbox-spring-app`) for the repair message
of the order, and the order events topic in Kafka UI (`http://localhost:8082`) for an `ORDER_REPAIR` message. If the
consumer failed, follow [Kafka Poison Pill Inspection and Re-drive](kafka-poison-pill-handling.md).

## Reset

Remove all containers and volumes to start again with an empty database:

```bash
docker compose --profile app down -v
```
