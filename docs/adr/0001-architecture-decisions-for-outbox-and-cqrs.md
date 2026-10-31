# 1. Architectural Strategy for Reliable Eventual Consistency

## Status

Accepted

## Context

Our application requires a reliable way to persist core business operations (specifically placing an order) while
simultaneously notifying downstream reporting and analytics services.

Doing a sequential "dual-write" (writing directly to PostgreSQL and immediately publishing to Apache Kafka within a
single standard web thread) introduces severe network and delivery vulnerabilities:

* If the database commit succeeds but Kafka crashes, events are permanently lost.
* If Kafka accepts the event but the database transaction rolls back, downstream services act on corrupted, ghost data.

Furthermore, mixing complex, query-heavy reporting schemas with low-latency operational transactional schemas degrades
database checkout performance and couples independent bounded contexts.

## Decisions

### 1. Implementation of the Transactional Outbox Pattern

We will use the Transactional Outbox Pattern to achieve bulletproof *at-least-once* message delivery. Instead of
communicating with two infrastructure components at once, the application will write both the `Order` entity and an
`OutboxEvent` log entry into the same PostgreSQL instance inside a single atomic database transaction.

### 2. Leverage Debezium for Change Data Capture (CDC)

An independent Debezium Connect container will asynchronously tail-read the PostgreSQL Write-Ahead Log (WAL). It will
capture row insertions in the `outbox` table and safely stream those payloads to Apache Kafka without putting load on
our synchronous Spring Boot HTTP thread.

### 3. Apply Command Query Responsibility Segregation (CQRS)

We strictly separate mutation operations from retrieval operations at the application service layer:

* **Command Side:** Handled via explicit, immutable `Command` objects processed by dedicated mutation services (e.g.,
  `CreateOrderService`). They trigger transactions, change states, and produce outbox entries.
* **Query Side:** Handled via immutable `Query` objects processed by read-only services (e.g., `GetOrdersService`). They
  read data without triggering domain side effects, bypass outbox generation, and leverage
  `@Transactional(readOnly = true)` optimizations.

### 4. Group by Domain-Driven Design (DDD) Bounded Contexts

We divide the application into two cleanly segregated bounded contexts using the "Package-by-Feature" approach:

* `order`: Manages the synchronous operational lifecycle of transactions.
* `history`: Manages the asynchronous analytical data warehouse reports, decoupled and populated via a Kafka
  `@KafkaListener`.

We divide each bounded context explicitly into three layers using internal subpackages. It enforces a clean separation
of concerns and maintains a highly maintainable architecture. The packages are:

* **`domain`**: Contains pure business models, entities, value objects, and repository interfaces, free from framework
  dependencies.
* **`application`**: Houses the use-case orchestration layer, including immutable Commands, Queries, and application
  services (e.g., `CreateOrderService`, `GetOrdersService`).
* **`infrastructure`**: Implements technical details, framework adapters, web routing (`web/` controllers), messaging
  drivers (`kafka/`), and real database connectors (`database/` via Spring Data).

### 5. Standardize on Spring Data JDBC Over Spring Data JPA (Hibernate)

We choose Spring Data JDBC for database interactions. Its lack of a first-level caching persistence context, missing
lazy-loading proxies, and predictable, immediate SQL compilation ensures that outbox rows are committed to the
transaction log exactly when written, allowing Debezium to pick up events immediately and reliably.

## Consequences

### Positive

* **Atomicity:** Zero risk of "dual-write" consistency failures; either everything succeeds or everything rolls back.
* **Performance:** The synchronous HTTP API response remains blazing fast because it doesn't wait for network roundtrips
  to Kafka.
* **Maintainability:** Clear separation between writes (Commands) and reads (Queries) makes testing highly
  straightforward.
* **Scalability:** Bounded contexts can easily be ripped apart into physical independent microservices later if
  necessary.

### Negative / Trade-offs

* **Eventual Consistency:** The analytical `order_history` report is no longer real-time; there is a
  microsecond-to-second propagation delay depending on infrastructure performance.
* **Infrastructure Complexity:** Requires managing and monitoring additional components like Debezium Connect and a full
  Kafka broker infrastructure cluster.
