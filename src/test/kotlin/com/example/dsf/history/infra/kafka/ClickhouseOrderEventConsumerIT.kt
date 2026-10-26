package com.example.dsf.history.infra.kafka

import com.example.dsf.commons.OrderStatus
import com.example.dsf.config.KafkaConfig
import com.example.dsf.history.infra.db.ClickhouseOrderHistoryRepository
import com.example.dsf.order.application.CreateOrderService
import com.example.dsf.order.application.UpdateOrderService
import com.example.dsf.order.application.commands.PlaceOrderCommand
import com.example.dsf.order.application.commands.UpdateOrderCommand
import com.example.dsf.utils.ClickhouseOrderHistoryTestRepository
import com.example.dsf.utils.IntegrationTestContainers
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID

@SpringBootTest
@TestPropertySource(
    properties = [
        "app.toggle.order-history-source=clickhouse",
        "spring.kafka.consumer.group-id=order-history-clickhouse-it",
    ],
)
class ClickhouseOrderEventConsumerIT : IntegrationTestContainers() {
    @Value($$"${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Autowired
    private lateinit var historyRepository: ClickhouseOrderHistoryTestRepository

    @Autowired
    private lateinit var createOrderService: CreateOrderService

    @Autowired
    private lateinit var updateOrderService: UpdateOrderService

    @Test
    fun `should process end to end flow from createOrderService through outbox cdc and consumer to history`() {
        val customerId = "customer-e2e-${UUID.randomUUID()}"
        val totalAmount = BigDecimal("149.99")

        val order =
            createOrderService.execute(
                PlaceOrderCommand(
                    customerId = customerId,
                    totalAmount = totalAmount,
                ),
            )

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findAll().filter { it.orderId == order.id }
            assertThat(records).isNotEmpty()
            val record = records.first()
            assertThat(record.customerId).isEqualTo(customerId)
            assertThat(record.totalAmount).isEqualByComparingTo(totalAmount)
            assertThat(record.orderStatus).isEqualTo(OrderStatus.CREATED)
        }
    }

    @Test
    fun `should process order status update end to end from updateOrderService through outbox cdc and consumer to history`() {
        val customerId = "customer-update-${UUID.randomUUID()}"
        val totalAmount = BigDecimal("79.50")
        val order = createOrderService.execute(PlaceOrderCommand(customerId, totalAmount))
        var createdVersion = -1L
        lateinit var createdAt: Instant

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findAll().filter { it.orderId == order.id }
            assertThat(records).hasSize(1)
            val createdRecord = records.first()
            assertThat(createdRecord.orderStatus).isEqualTo(OrderStatus.CREATED)
            assertThat(createdRecord.customerId).isEqualTo(customerId)
            assertThat(createdRecord.totalAmount).isEqualByComparingTo(totalAmount)
            createdVersion = createdRecord.version
            createdAt = createdRecord.createdAt
        }

        val timeBeforeUpdate = Instant.now()
        Thread.sleep(10)

        updateOrderService.execute(
            UpdateOrderCommand(
                orderId = order.id,
                customerId = customerId,
                newStatus = OrderStatus.PROCESSING,
            ),
        )

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findAll().filter { it.orderId == order.id }
            assertThat(records).hasSize(1)
            val record = records.first()
            assertThat(record.orderStatus).isEqualTo(OrderStatus.PROCESSING)
            assertThat(record.version).isGreaterThan(createdVersion)
            assertThat(record.customerId).isEqualTo(customerId)
            assertThat(record.totalAmount).isEqualByComparingTo(totalAmount)
            assertThat(record.createdAt).isEqualTo(createdAt)
            assertThat(record.updatedAt).isAfter(timeBeforeUpdate)
        }

        val snapshotBeforeUpdate = historyRepository.findBefore(timeBeforeUpdate).single { it.orderId == order.id }
        assertThat(snapshotBeforeUpdate.orderStatus).isEqualTo(OrderStatus.CREATED)
        assertThat(snapshotBeforeUpdate.version).isEqualTo(createdVersion)

        val snapshotNow = historyRepository.findBefore(Instant.now()).single { it.orderId == order.id }
        assertThat(snapshotNow.orderStatus).isEqualTo(OrderStatus.PROCESSING)
        assertThat(snapshotNow.createdAt).isEqualTo(createdAt)

        // Background merges must not remove earlier states, otherwise time travel loses them
        historyRepository.optimizeFinal()
        val snapshotAfterMerge = historyRepository.findBefore(timeBeforeUpdate).single { it.orderId == order.id }
        assertThat(snapshotAfterMerge.orderStatus).isEqualTo(OrderStatus.CREATED)
    }

    @Test
    fun `should consume raw JSON order event without type headers and save history`() {
        val orderId = UUID.randomUUID()
        val customerId = "customer-123"
        val rawJsonPayload =
            """
            {
                "type": "ORDER_CREATED",
                "orderId": "$orderId",
                "customerId": "$customerId",
                "totalAmount": "99.99",
                "status": "CREATED",
                "createdAt": "2026-01-15T10:00:00Z",
                "updatedAt": "2026-01-15T10:00:00Z"
            }
            """.trimIndent()

        val producerProps =
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            )

        KafkaProducer<String, String>(producerProps).use { producer ->
            producer.send(ProducerRecord(KafkaConfig.ORDER_EVENTS_TOPIC, customerId, rawJsonPayload)).get()
        }

        // Wait and verify order history entry
        await.atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).untilAsserted {
            val records = historyRepository.findAll().filter { it.orderId == orderId }
            assertThat(records).isNotEmpty()
            val record = records.first()
            assertThat(record.customerId).isEqualTo(customerId)
            assertThat(record.totalAmount).isEqualByComparingTo("99.99")
            assertThat(record.orderStatus).isEqualTo(OrderStatus.CREATED)
        }
    }

    @Test
    fun `should route poison pill raw payload to DLT topic without serialization exceptions`() {
        val poisonPayload = "invalid-non-json-byte-payload".toByteArray(Charsets.UTF_8)
        val testKey = "poison-key-${UUID.randomUUID()}"

        val producerProps =
            mapOf(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name,
            )

        val dltConsumerProps =
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG to "test-dlt-verifier-group-${UUID.randomUUID()}",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name,
            )

        KafkaConsumer<String, ByteArray>(dltConsumerProps).use { dltConsumer ->
            dltConsumer.subscribe(listOf(KafkaConfig.ORDER_EVENTS_DLT_TOPIC))

            // Initial poll to join group / partition assignment
            dltConsumer.poll(Duration.ofMillis(500))

            KafkaProducer<String, ByteArray>(producerProps).use { producer ->
                producer.send(ProducerRecord(KafkaConfig.ORDER_EVENTS_TOPIC, testKey, poisonPayload)).get()
            }

            val received = mutableListOf<ConsumerRecord<String, ByteArray>>()
            await.atMost(Duration.ofSeconds(15)).pollInSameThread().untilAsserted {
                received += dltConsumer.poll(Duration.ofMillis(500)).filter { it.key() == testKey }
                assertThat(received).isNotEmpty()
            }
            assertThat(received.first().value()).isEqualTo(poisonPayload)
        }
    }
}
