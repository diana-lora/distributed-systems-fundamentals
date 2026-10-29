## 🐳 Deployment Guide via Docker Compose (Container Mode)

This section describes how to spin up the complete ecosystem (Database, Kafka, Debezium, and the Spring Boot
application) fully automated inside Docker containers. This is the ideal setup for continuous integration (CI)
environments or quick demonstrations.

### 1. Prerequisites

Before starting, ensure you compile the application executable binary (`.jar`) so Docker can build the image correctly:

```bash
./gradlew clean bootJar
```

### 2. Standard Stack Initialization

Run the following command in your terminal to build the local images and spin up all services in the background:

```bash
docker compose up --build -d
```

On the first start, the application inserts demo orders into PostgreSQL (`APP_DEMO_DATA_ENABLED`), which flow through
Debezium and Kafka into the ClickHouse order history. The
[order history scenarios runbook](../docs/runbook/clickhouse-order-history-scenarios.md) uses them. Run
`docker compose down -v` to start again from a clean database.

### 3. Service Access Ports

Once the ecosystem is fully initialized, you can verify the status of the components through the following local
addresses:

* **Spring Boot Application (REST API):** `http://localhost:8080`
* **Health Check (Actuator):** `http://localhost:8080/actuator/health`
* **Kafka Visual Interface (Kafka UI):** `http://localhost:8082`
* **Kafka Connect REST API (Debezium):** `http://localhost:8083/connectors`

---

## 💻 Configuration & Workflow for Local Development with IntelliJ IDEA

If you want to modify source code, debug features, or run interactive tests in real-time, you should run the Spring Boot
application natively within **IntelliJ IDEA** and delegate only the heavy infrastructure to Docker Compose.

### 1. Required Adjustments in `docker-compose.yml`

To prevent port conflicts (such as `address already in use` on port 8080) and initialization order issues, you must
decouple the `app` service block from the core infrastructure dependencies. Make the following changes:

* **Remove Application Dependency:** Locate the `debezium` service definition and completely remove the `app` block
  reference from its `depends_on` list. Debezium only requires the database and the broker to be ready to spin up its
  REST interface.
* **Keep Core Health Checks:** Ensure that the `debezium` service's `depends_on` property remains mapped exclusively to
  `postgres` and `kafka` with a `condition: service_healthy` rule.

### 2. Step-by-Step Startup Sequence for Hybrid Development

To work smoothly with IntelliJ without triggering Debezium schema validation failures, you must follow this strict
sequential order:

**Step 2.1: Spin up the core infrastructure**
Launch only the database engine, messaging broker, and visual interface by running:

```bash
docker compose up postgres kafka kafka-ui -d
```

**Step 2.2: Start the application in IntelliJ IDEA**
Open the project in IntelliJ and execute the main application class using the **Run** or **Debug** button. Upon startup,
the app connects directly to the database via `localhost:5432`, triggers **Flyway**, and creates the complete production
schema and business tables (e.g., `orders`, `outbox`).

**Step 2.3: Activate Debezium Change Data Capture (CDC)**
Once the IntelliJ console confirms that the application is running successfully on port 8080, run the final command to
activate the streaming connectors:

```bash
docker compose up debezium connector-init -d
```

The automated curl script will register your connector JSON structure. Because Flyway already generated the real tables
in the previous step, Debezium will immediately map the schema layout cleanly without missing-table errors.
