# ADR: Resilient Kafka Consumer Error Handling via Dead Letter Queue

## Status

Accepted

## Context and Problem Statement

Our system uses the Outbox Pattern via Debezium CDC to stream database changes into Apache Kafka. The order history
consumer group reads these event payloads from the order events topic to build a read model.

In an event driven system, messaging consumer architectures face transient failures and fatal failures. Transient
failures include database timeouts or temporary network disconnections. Fatal failures include malformed JSON structures
or unmappable data types. We call these fatal invalid messages poison pills.

By default, when an unhandled exception or serialization error occurs, the Spring Kafka consumer thread falls into an
infinite retry loop on the exact same message offset. This behavior blocks the ingestion partition completely. It
prevents the processing of next valid messages and causes severe consumer lag. We need a standardized approach to
isolate unprocessable records without bringing down the ingestion pipeline.

## Decision and Technical Rules

We will implement an automated Dead Letter Queue architecture pattern. The implementation uses the DefaultErrorHandler
combined with the DeadLetterPublishingRecoverer and the ErrorHandlingDeserializer components.

| Component                  | Responsibility                                                                                                                 |
|----------------------------|--------------------------------------------------------------------------------------------------------------------------------|
| Error Isolation            | Wrap the native JsonDeserializer within a Spring ErrorHandlingDeserializer to trap low level parsing issues.                   |
| Retry Strategy             | Configure a standard retry threshold of 3 attempts with a fixed back-off delay interval set to 2000 milliseconds.              |
| Fatal Exception Exclusions | Classify SerializationException and DeserializerException as non retryable exceptions to bypass recovery loops.                |
| DLQ Destination Naming     | Standardize the dead letter topic names using the target topic base string appended with a dot DLT (Dead Letter Topic) suffix. |

The main topic name is order events. The dead letter queue topic name is order events DLT.

## Consequences

### Positive Impacts

* Pipeline Isolation: Corrupted entries shift immediately to the dead letter target log. The ingestion partition stays
  active and processes next sequential messages.
* Operational Auditing Visibility: Support teams can inspect invalid structures directly from the DLT topic stream log.
* Strict Thread Safety Bounds: System failures do not trigger memory leaks or block background tasks.

### Negative Impacts

* Out of Order Execution Context Risk: If a message fails and shifts into the DLT topic while the next message processes
  successfully, the sequence changes.
* Storage Cluster Overhead: The infrastructure requires extra storage volume boundaries to handle data persistence on
  the DLT topic.