# ADR: ClickHouse Mutation & Optimization Strategies for Order History

## Status

Accepted

## Context

In our system, our transactional database (PostgreSQL) stores data in rows. This row-based structure makes changing a
single cell with an `UPDATE` or `DELETE` statement very fast. However, when PostgreSQL has to scan millions of rows to
calculate business metrics or historical trends, it slows down significantly.

To get fast analytics, we added **ClickHouse** as our analytical datastore (`OLAP`). ClickHouse compresses and
stores each column in its own separate file on disk. This columnar file structure makes reading specific fields
incredibly fast, but it makes changing data very slow. If you update a single value, ClickHouse must decompress the
whole column file, rewrite the data, and compress it back to disk.

To keep ClickHouse running at peak performance, we cannot use standard relational CRUD habits. Instead, we must write
code that relies on appending new data, targeting specific file folders, or using in-memory calculations. We need a
design that satisfies the following five specific core business requirements while ensuring that Historical
Point-in-Time Reconstruction (Time Travel) remains 100% accurate and operational across all of them:

1. **Order Creation:** Capture new orders streamed from Kafka instantly.
2. **Order Status Updates:** Track rapid operational state transitions (e.g., `CREATED` to `PROCESSING`) without causing
   database write blocks.
3. **Retroactive Data Corrections:** Correct financial data errors (e.g., `totalAmount` typos) in a way that modifies
   past records so that historical reporting remains accurate.
4. **Historical Point-in-Time Reconstruction (Time Travel):** Rebuild a clean snapshot of all orders exactly as they
   looked at any specific second in the past.
5. **History Rebuilds (Discrepancy Troubleshooting):** Provide an endpoint for developers to fix rare out-of-sync data
   discrepancies between PostgreSQL and ClickHouse manually.

---

## Decision

We chose to model our ClickHouse implementation by mapping each scenario to a specific optimization mechanism that
honors columnar storage constraints:

### Scenario 1: Order Creation

* **Options in ClickHouse:** Standard `INSERT` operation.
* **Our Choice: Direct Append-Only `INSERT`**.
* **Why we chose it:** Adding new data to the end of a column file is the fastest operation ClickHouse can do.
  ClickHouse writes the data block straight to disk without checking constraints or locking rows, keeping CPU and disk
  usage near zero. We map the row's `version` directly to the unique, incremental **Kafka Partition Message
  Offset** to establish an absolute ordering timeline.
* **Performance & Computational Cost:** **Near Zero.** Linear disk writes require minimal CPU and memory overhead
  because ClickHouse avoids transaction logs, constraints, and page locks.

### Scenario 2: Order Status Update (`CREATED` → `PROCESSING`)

* **Options in ClickHouse:**
    1. *Relational Approach:* Run a heavy SQL mutation
       (`UPDATE order_history SET status = 'PROCESSING' WHERE order_id = ...`).
    2. *Deduplication Approach:* Use the `ReplacingMergeTree` engine and insert changes as new rows.
* **Our Choice:** **Append-Only Ingestion via `ReplacingMergeTree` with `argMax()` memory aggregation**.
* **Why we chose it:** A traditional `UPDATE` query triggers an expensive database mutation that slows down ingestion.
  Instead, we use the **`ReplacingMergeTree(version)`** engine. We **insert a new row** with the new
  status and a higher version number (the next Kafka offset). ClickHouse handles this as a cheap, instant
  insert. The sorting key `ORDER BY (customer_id, order_id, updated_at)` includes the time of each status change, so
  every state of the order is a distinct row that background merges **keep**; this is what makes time travel possible.
  Merges only remove rows that share the full key, i.e. the same `updated_at`: Kafka redeliveries and the corrupted
  rows replaced by a backdated rebuild (Scenario 5). Because `ReplacingMergeTree` only deduplicates rows inside the
  same partition, the table is partitioned by the immutable order creation time `toYYYYMM(created_at)`, so every
  version of an order lands in the same partition. To read the latest status, our repository uses a fast in-memory
  aggregation query instead of the heavy `FINAL` keyword:
  ```sql
  SELECT order_id, argMax(status, version) AS status FROM order_history GROUP BY order_id;
  ```
* **Performance & Computational Cost:** **Low Write Cost / Low-to-Medium Read Cost.** Ingestion remains instantaneous
  since updates are treated as raw inserts. Reads incur a minor CPU overhead during query execution because
  ClickHouse must collapse row versions in memory using RAM aggregation.
* **Worked Example:** [Scenario 2 in the design note](../design/clickhouse-order-history-scenarios.md#scenario-2-order-status-update).

### Scenario 3: Retroactive Total Amount Correction

* **Options in ClickHouse:**
    1. *Classic Mutation Structure:* Run an `ALTER TABLE ... UPDATE` statement to force a physical column file rewrite
       on disk.
    2. *Lightweight Update Patching:* Run a standard `UPDATE ... SET` query to generate highly compressed "patch
       parts".
* **Our Choice:** **Lightweight Update Patching**.
* **Why we chose it:** To avoid the computational heavy-lifting of re-compressing entire column files on disk, we
  utilize ClickHouse's native lightweight updates. Because human data corrections are low-volume anomalies
  impacting less than 1% of the total dataset, creating tiny patch deltas is drastically more efficient than initiating
  global background table mutations. We use a standard SQL `UPDATE` statement and enforce a **strict partition
  rule** in the application to optimize indexing:
  ```sql
  UPDATE order_history 
  SET total_amount = ? 
  WHERE customer_id = ? AND order_id = ?
    AND created_at >= :partitionStart AND created_at < :partitionEnd; -- Limits the patch to one partition
  ```
* **Time Travel Verification:** Because patch parts link directly to target rows, they honor the primary key and
  historical indexes. Any point-in-time evaluation automatically applies the active patch, ensuring financial reporting
  accuracy remains impeccable.
* **Performance & Computational Cost:** **Low Write Cost / Minor Read Overhead.** Writing a patch part happens instantly
  without impacting ingestion pipelines. Time travel read operations absorb a very minor computation penalty to stitch
  column deltas together in RAM before background data merges complete.
* **Worked Example:** [Scenario 3 in the design note](../design/clickhouse-order-history-scenarios.md#scenario-3-retroactive-total-amount-correction).

### Scenario 4: Historical Point-in-Time Reconstruction (Time Travel)

* **Options in ClickHouse:**
    1. *Application-Level Filtering:* Pull all duplicate historical records into the application and filter them in
       memory.
    2. *Database-Level Merging (`FINAL`):* Use `SELECT * FROM order_history FINAL WHERE updated_at <= ?`.
    3. *Index Aggregation:* Use a timestamp filter with an in-memory `argMax` group operation.
* **Our Choice:** **Timestamp Filter with Memory-Based `argMax` Aggregation**.
* **Why we chose it:** Pulling raw duplicates into the application destroys network bandwidth, and the `FINAL`
  modifier forces heavy disk merges at runtime. By combining a `WHERE created_at <= ? AND updated_at <= ?` clause with
  `GROUP BY order_id` and `argMax(column, version)`, ClickHouse drops unwanted future time segments instantly: the
  `created_at` filter prunes whole monthly partitions of orders that did not exist yet, and the `updated_at` filter
  removes state changes that happened after the target time. It then collapses duplicate rows directly in RAM, returning
  a clean, lightweight list containing
  only one row per order.
* **Performance & Computational Cost:** **Medium Read Cost.** The query utilizes primary keys and columnar index
  structures to drop unwanted time slices instantly from disk. Processing data grouping and version selection
  requires temporary RAM allocation during execution.
* **Worked Example:** [Shared foundation in the design note](../design/clickhouse-order-history-scenarios.md#shared-foundation-the-three-columns-that-drive-time-travel),
  which explains the three columns and the time travel query used by every snapshot.

### Scenario 5: History Rebuilds (Discrepancy Troubleshooting)

* **Options in ClickHouse:**
    1. *Append-Only Re-injection with Higher Version:* Insert the correct data directly from PostgreSQL using an
       incremented `version` value but preserving the original historical timestamps.
    2. *Atomic Replacement via Temporary Partition:* Load clean data into a buffer table and swap files using
       `ALTER TABLE ... REPLACE PARTITION`.
    3. *Lightweight Delete followed by Re-insert:* Delete every row of the order, then insert the correct timeline.
* **Our Choice:** **Append-Only Re-injection of the Full Timeline, followed by a Targeted Lightweight `DELETE`**.
* **Why we chose it:** Wiping a full month folder via metadata (`DROP PARTITION`) is too destructive if only a few rows
  suffer from data drift. Atomic replacement via temporary partitions requires managing buffer schemas, which adds
  unnecessary structural complexity. Deleting before re-inserting leaves the order without rows between the two
  statements. Instead, the order domain keeps the status timeline of every order in PostgreSQL and publishes the whole
  timeline as one repair event. The history appends every step, backdated to its original `updated_at`, with the
  Kafka offset of the repair event as version. Backdated rows replace the drifted rows through background merges.
  Rows that no repaired row replaces (orphans: a state that never happened, a wrong `customer_id` or a wrong
  `created_at`) are removed afterwards by one targeted lightweight delete:
  ```sql
  DELETE FROM order_history WHERE order_id = ? AND version < ?; -- ? = Kafka offset of the repair event
  ```
* **Performance & Computational Cost:** **Low Write Cost / Low, One-Time Deletion Cost.**
    * *Insert:* Identical to standard append ingestion. All steps are written as one `INSERT` block, which
      ClickHouse applies atomically.
    * *Lightweight `DELETE`:* No column files are rewritten. ClickHouse evaluates the `WHERE` clause and writes only
      the hidden `_row_exists` mask for the data parts that contain matching rows. Because `order_id` is not the
      leading column of the sorting key and the statement has no `created_at` filter, the primary index cannot skip
      parts: ClickHouse reads the `order_id` and `version` columns of **every** part of the table. This full column
      scan is the price for also removing orphans in other partitions.
    * *Reads:* Until a background merge physically drops the masked rows, every read applies the mask as an implicit
      filter, a minor overhead comparable to the patch parts of Scenario 3.
    * *Why it is acceptable:* Repairs are rare, manual, single-order operations. A classic `ALTER TABLE ... DELETE`
      mutation would rewrite all columns of every affected part. The lightweight delete only scans two columns and
      writes a small mask.
    * *Limit:* Frequent repairs would repeat the full-table scan and accumulate masks and merge load, so the rebuild
      must remain a troubleshooting tool, not part of the regular ingestion.
* **Worked Example:** [Scenario 5 in the design note](../design/clickhouse-order-history-scenarios.md#scenario-5-history-rebuild-with-backdated-versions).

---

## Consequences

### Positive Impact

* **Accurate Time Travel:** Every state change is kept as its own row (`updated_at` in the sorting key), so snapshots
  return the state valid at the target time, including retroactive amount corrections and backdated rebuilds.
* **High Write Throughput:** Order creation and status updates are plain inserts without row locks or mutations.
* **Fast Reporting Without `FINAL`:** Snapshots prune partitions by `created_at` and resolve versions in memory with
  `argMax`.
* **Non-Blocking Troubleshooting:** Rebuilds are an insert plus a lightweight delete; neither blocks ingestion or
  reads, and the repaired history is visible before the delete runs.
* **Clean Rebuilds:** A rebuild replaces the full timeline of an order and removes orphan rows, independent of their
  timestamps or partitions.

### Negative Impact

* **Growing Storage per Order:** Every status change is kept as its own row to enable time travel, so storage grows
  with the number of state changes, not with the number of orders. Only true duplicates (same `updated_at`) are
  removed by background merges.
* **Growing Read Cost:** `argMax` aggregates all versions of an order up to the target time, so the cost of a snapshot
  grows with the number of status changes.
* **Patch Part Accumulation (Scenario 3):** Each lightweight `UPDATE` creates a patch part that reads must apply until a
  background merge folds it into the data parts. Frequent corrections increase read overhead and merge load, so
  corrections must remain low-frequency events.
* **Beta Feature Dependency (Scenario 3):** Lightweight updates are a beta feature since ClickHouse 25.8 and require the
  table settings `enable_block_number_column` and `enable_block_offset_column`. ClickHouse upgrades must be checked for
  changes to this feature.
* **Full-Table Scan per Rebuild (Scenario 5):** The lightweight `DELETE` filters by `order_id` without a partition
  filter, so it reads the `order_id` and `version` columns of every part. Masked rows add a read overhead until
  background merges remove them, so rebuilds must remain rare troubleshooting operations.
* **Status Timeline in PostgreSQL (Scenario 5):** Rebuilds depend on a status timeline in PostgreSQL, which must be
  written in the same transaction as every status change and hold the exact `updated_at` of the published event.
* **Version Coupled to Kafka Offsets:** Ordering per order relies on all its events sharing one partition (keyed by
  `customer_id`). Offsets restart if the topic is recreated; a rebuild after a recreation would no longer have a
  higher version than the rows it replaces, and its delete would not remove them (see Scenario 5).

---

## TL;DR

| Scenario             | Our Goal                 | Technical Pattern We Chose                 | Why It Performs Better                                                                                                                                                                                                                              |
|:---------------------|:-------------------------|:-------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **1. Creation**      | Fast data intake         | **`INSERT`**                               | **Append-Only Speed:** Writes data directly to the end of files without checking row constraints.                                                                                                                                                   |
| **2. Status Update** | Real-time tracking       | **`ReplacingMergeTree` + `argMax()`**      | **No Database Locks:** Converts slow updates into instant inserts that keep every state (`updated_at` in the sorting key) and resolves the current state in memory during reads.                                                                     |
| **3. Correction**    | Accurate Time Travel     | **`UPDATE ... SET` (Lightweight Update)**  | **Patch-on-Read Delta:** Drops write costs to near-zero by writing tiny delta patches, merging them into main columns later during background maintenance.                                                                                          |
| **4. Time Travel**   | Past snapshot auditing   | **`WHERE` Timestamp + `argMax()`**         | **In-Memory Aggregation:** Uses index filtering to drop unwanted records fast and collapses versions in RAM.                                                                                                                                        |
| **5. Rebuild**       | Resolve out-of-sync data | **`INSERT` v+ then Lightweight `DELETE`**  | **Low Deletion Cost + Accurate Time Travel:** Inserts the full timeline with a higher version, backdated to the original timestamps, then masks every older row (orphans included) instead of rewriting column files. Background merges drop them physically. |
