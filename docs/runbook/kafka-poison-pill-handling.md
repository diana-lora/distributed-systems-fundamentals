# Runbook: Kafka Poison Pill Inspection and Re-drive Procedure

## Purpose
This document describes how to handle unprocessable messages trapped in the Dead Letter Queue. Follow these steps to inspect invalid payloads and re-drive them safely into the system.

## Ingestion Pipeline Overview
The Debezium connector streams events from the outbox table to the order events topic. If the consumer group encounters a fatal serialization failure, the error handler routes the record to the order events DLT topic.

## Step 1: How to Inspect a Poison Pill in Kafka UI
1. Open your web browser and navigate to the Kafka UI console at http://localhost:8082.
2. Click on the Topics option in the left navigation sidebar.
3. Select the order events DLT topic from the displayed list.
4. Click on the Messages tab to view the failed records.
5. Expand the latest message payload to read the raw JSON text.
6. Click on the Headers tab of that message. Spring automatically attaches metadata here. Read the exception message header to identify the exact parsing error.

## Step 2: How to Move a Message from DLQ back to the Main Topic
1. Copy the raw payload content of the poison pill from the Kafka UI messages view.
2. Paste the payload into a local text editor. Correct the invalid data properties or formatting errors.
3. Go back to the Kafka UI console. Click on the Topics option in the left sidebar.
4. Select the main destination topic named order events.
5. Click on the Produce Message button located in the top right corner.
6. Paste your corrected JSON payload into the Value text input field.
7. Enter the original key in the Key field to preserve partition routing constraints.
8. Click on the Submit button to publish the corrected message. The consumer group will process the record immediately.
