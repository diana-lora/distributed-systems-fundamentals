# Transactional Outbox Pattern with Spring Boot, Debezium, and Kafka

## 📌 What is this Repository About?
This repository is a practical implementation of the **Transactional Outbox Pattern** built with a **Spring Boot + Kotlin** application. The project serves as an architectural blueprint for decoupled microservices, demonstrating how to securely manage data persistence and asynchronous event distribution without falling into the common pitfalls of distributed systems.

## 🎯 What Problem Am I Trying to Practice?

### The Business Scenario: Order Placement vs. Order History Report
In a typical e-commerce system, when a customer places an order, two distinct business goals must be achieved:
1. **The Core Transaction (Sync):** The system must immediately charge the customer, reserve the stock, and save the order with a `PENDING` or `CONFIRMED` status. The user expects an instant confirmation on their screen.
2. **The Analytics / History Report (Async):** The system must update the customer's lifetime order history, recalculate loyalty reward points, and feed the data warehouse for business intelligence reports.

Generating complex reports or sync-writing to an analytical database directly inside the user's checkout request creates **tight coupling** and slows down the checkout API drastically. If the reporting service goes down, the checkout process crashes.

### The Architectural Problem (The "Dual-Write" Pitfall):
To keep these systems decoupled, a common approach is to make the Order Service save to its database and then send an asynchronous event (`OrderCreated`) to a Kafka topic so the Reporting Service can consume it.

However, doing this sequentially introduces the **Dual-Write Problem**:
* **Scenario A (Kafka Failure):** The Order Service saves the order to PostgreSQL successfully, but Kafka suffers a transient network hiccup. The API returns `201 Created` to the user, but the event is never sent. The customer's order history report is now permanently out of sync.
* **Scenario B (Database Failure):** The Order Service sends the event to Kafka first, but the database transaction rolls back due to a constraint violation. The Reporting Service creates a history report for an order that technically does not exist.

### The Solution via Transactional Outbox:
By placing an **Outbox** in the middle, we ensure **Eventual Consistency**:
1. The **Order Service** writes both the `Order` data and a corresponding `OrderCreatedEvent` record into the same PostgreSQL database inside a **single atomic transaction**.
2. If the database write succeeds, both records are guaranteed to be persisted. If it fails, everything rolls back.
3. **Debezium** tail-reads the database transaction log (WAL) instantly, extracts the outbox event, and streams it to Kafka.
4. The **Reporting Service** reads from Kafka asynchronously and updates the history database at its own pace. If the Reporting Service is down, Kafka stores the messages safely until it recovers. No events are ever lost, and the checkout API remains blazing fast.

---

## 🛠️ Technologies Used
The application stack is built entirely using containerized modern enterprise technologies:

* **Backend Framework:** Spring Boot 3.x with **Kotlin**
* **Build System:** Gradle (Kotlin DSL)
* **Database Management:** PostgreSQL 16 (Configured for Logical Replication)
* **Database Migrations:** Flyway
* **Message Broker:** Apache Kafka (Confluent Platform running in KRaft mode)
* **Change Data Capture (CDC):** Debezium Connect 2.7 (Customized image with Scripting SMT for dynamic topic routing)
* **Monitoring UI:** Provectus Kafka UI
* **Infrastructure Orchestration:** Docker & Docker Compose

---

## 🚀 How to Run the Application Locally

1. **Install Prerequisites**
   Ensure that **Java 21** (or compatible JDK) and **Docker Desktop / Engine** are installed and running on your machine.

2. **`./gradlew clean bootJar`**
   Compiles the Kotlin source code and generates the executable application archive inside the local build directory.

3. **`cd infra`**
   Navigates into the infrastructure folder containing the orchestrator configuration assets.

4. **`docker compose --profile app up --build -d`**
   Builds the custom Debezium image and spins up the entire architectural service stack in the background.

5. **Verify `http://localhost:8080/actuator/health`**
   Accesses the Spring Boot diagnostics interface to confirm the server, database, and broker connections are fully operational.

6. **Verify `http://localhost:8082`**
   Opens the Provectus Kafka UI control plane dashboard in your web browser to monitor topics and live event streams.

7. **Verify `http://localhost:8083/connectors`**
   Queries the Debezium Connect REST API directly to verify the connector engine status and view active configurations.

8. **`docker compose --profile app down`**
   Stops and removes all running infrastructure containers safely when you are finished developing.

9. **`docker compose --profile app down -v`**
   Stops the containers and permanently deletes the local PostgreSQL database data volumes to trigger a fresh Flyway migration on the next run.

---

## 🧹 Code Style

Kotlin code style is enforced with [ktlint](https://pinterest.github.io/ktlint/) (rules configured in `.editorconfig`). `ktlintCheck` runs as part of `./gradlew check`.

* **`./gradlew ktlintCheck`** - Reports code style violations.
* **`./gradlew ktlintFormat`** - Automatically fixes code style violations.

---

## 🔍 Troubleshooting & Useful Commands

* **`docker compose ps`**
  Displays the current operational state and port bindings of all containers to quickly verify which services are healthy or crashing.

* **ClickHouse log warnings**
  `Delay accounting is not enabled` and `Integrity check of the executable skipped` are harmless in the container: delay accounting depends on a kernel setting, and the official image may not include a reference checksum.

* **`docker compose logs app -f`**
  Tails the live log stream of the Spring Boot application (change `app` to `debezium` or `kafka` to inspect other services) to capture startup exceptions or connection failures.

* **`docker compose build --no-cache`**
  Forces Docker to completely wipe out its internal build memory layers and download all script assets fresh, bypassing corrupted local caches.

* **`docker compose --profile app down -v`**
  Stops all running containers and permanently deletes the local PostgreSQL database data volumes, allowing you to re-run Flyway migrations from scratch.

* **`curl http://localhost:8083/connectors`**
  Queries the Debezium engine directly to check if your outbox connector has been registered successfully and is currently monitoring the database transaction log.

---

## 🔌 Debezium Connector Configuration & Automation

To activate the Change Data Capture (CDC) pipeline, Debezium must be instructed to monitor our PostgreSQL database outbox table by posting a JSON configuration to its REST API. This setup utilizes the Debezium Outbox Event Router SMT (Single Message Transform) to strip database replication metadata and stream only the pure, inner JSON payload into the target Kafka topic.

First, create the configuration blueprint file named `infra/debezium-outbox-connector.json` with the following content:
```json
{
  "name": "YOUR_CONNECTOR_NAME",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "YOUR_DATABASE_HOST",
    "database.port": "YOUR_DATABASE_PORT",
    "database.user": "YOUR_DATABASE_USER",
    "database.password": "YOUR_DATABASE_PASSWORD",
    "database.dbname": "YOUR_DATABASE_NAME",
    "database.server.name": "YOUR_LOGICAL_SERVER_NAME",
    "plugin.name": "pgoutput",
    "table.include.list": "public.outbox",
    "transforms": "outbox",
    "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
    "transforms.outbox.route.by.field": "target_topic",
    "transforms.outbox.route.topic.replacement": "${routedByValue}",
    "transforms.outbox.key.field": "aggregate_key",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

To deploy this configuration completely unattended on startup, include this wrapper initialization container inside your `infra/docker-compose.yml` stack, which polls the Debezium REST API and submits the JSON configuration automatically as soon as the service is ready:
```yaml
  connector-init:
    image: curlimages/curl:8.7.1
    container_name: outbox-connector-init
    volumes:
      - ./debezium-outbox-connector.json:/tmp/config.json
    depends_on:
      debezium:
        condition: service_started
    command: >
      sh -c "
        echo 'Waiting for Debezium REST API to accept traffic...'
        while ! curl -s http://debezium:8083/connectors; do
          sleep 2
        done
        echo 'Submitting outbox connector payload configuration...'
        curl -X POST -H 'Content-Type: application/json' --data @/tmp/config.json http://debezium:8083/connectors
        echo 'Registration successful!'
      "
```

Alternatively, if you prefer submitting the registration manually from your host machine terminal instead of using the automated initialization container, execute this fallback command:
```bash
curl -X POST -H "Content-Type: application/json" --data @debezium-outbox-connector.json http://localhost:8083/connectors
```
