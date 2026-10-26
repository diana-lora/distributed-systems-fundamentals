package com.example.dsf.history.infra.kafka

import com.example.dsf.commons.OrderStatus
import com.example.dsf.config.KafkaConfig
import com.example.dsf.history.infra.db.PostgresOrderHistoryRepository
import com.example.dsf.order.application.CreateOrderService
import com.example.dsf.order.application.UpdateOrderService
import com.example.dsf.order.application.commands.PlaceOrderCommand
import com.example.dsf.order.application.commands.UpdateOrderStatusCommand
import com.example.dsf.order.application.commands.UpdateOrderTotalAmountCommand
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
import java.util.UUID

@SpringBootTest
@TestPropertySource(
    properties = [
        "app.toggle.order-history-source=postgres",
        "spring.kafka.consumer.group-id=order-history-postgres-it",
    ],
)
class PostgresOrderEventConsumerIT : IntegrationTestContainers() {
    @Value($$"${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Autowired
    private lateinit var historyRepository: PostgresOrderHistoryRepository

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

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findAll().filter { it.orderId == order.id }
            assertThat(records).anySatisfy { record ->
                assertThat(record.orderStatus).isEqualTo(OrderStatus.CREATED)
            }
        }

        updateOrderService.execute(
            UpdateOrderStatusCommand(
                orderId = order.id,
                customerId = customerId,
                newStatus = OrderStatus.PROCESSING,
            ),
        )

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findAll().filter { it.orderId == order.id }
            assertThat(records).hasSize(2)
            assertThat(records.map { it.orderStatus })
                .containsExactlyInAnyOrder(OrderStatus.CREATED, OrderStatus.PROCESSING)
            assertThat(records.maxBy { it.version }.orderStatus).isEqualTo(OrderStatus.PROCESSING)
            assertThat(records).allSatisfy { record ->
                assertThat(record.customerId).isEqualTo(customerId)
                assertThat(record.totalAmount).isEqualByComparingTo(totalAmount)
            }
        }
    }

    @Test
    fun `should correct total amount in every history version of the order`() {
        val customerId = "customer-amount-${UUID.randomUUID()}"
        val wrongAmount = BigDecimal("99.00")
        val correctAmount = BigDecimal("9.90")
        val order = createOrderService.execute(PlaceOrderCommand(customerId, wrongAmount))

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            assertThat(historyRepository.findAll().filter { it.orderId == order.id }).hasSize(1)
        }

        updateOrderService.execute(
            UpdateOrderStatusCommand(
                orderId = order.id,
                customerId = customerId,
                newStatus = OrderStatus.PROCESSING,
            ),
        )

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            assertThat(historyRepository.findAll().filter { it.orderId == order.id }).hasSize(2)
        }

        updateOrderService.execute(
            UpdateOrderTotalAmountCommand(
                orderId = order.id,
                customerId = customerId,
                newTotalAmount = correctAmount,
            ),
        )

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findAll().filter { it.orderId == order.id }
            // The correction patches existing rows instead of appending a new version
            assertThat(records).hasSize(2)
            assertThat(records.map { it.orderStatus })
                .containsExactlyInAnyOrder(OrderStatus.CREATED, OrderStatus.PROCESSING)
            assertThat(records).allSatisfy { record ->
                assertThat(record.totalAmount).isEqualByComparingTo(correctAmount)
            }
        }
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
