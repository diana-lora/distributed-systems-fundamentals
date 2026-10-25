package com.example.dsf.history.infra.kafka

import com.example.dsf.config.KafkaConfig
import com.example.dsf.history.infra.db.PostgresOrderHistoryRepository
import com.example.dsf.order.application.CreateOrderService
import com.example.dsf.order.application.commands.PlaceOrderCommand
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
import com.example.dsf.utils.IntegrationTestContainers
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID

@SpringBootTest
@TestPropertySource(properties = [
    "app.toggle.order-history-source=postgres",
    "spring.kafka.consumer.group-id=order-history-postgres-it"
])
class PostgresOrderEventConsumerIT : IntegrationTestContainers() {

    @Value($$"${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Autowired
    private lateinit var historyRepository: PostgresOrderHistoryRepository

    @Autowired
    private lateinit var createOrderService: CreateOrderService

    @Test
    fun `should process end to end flow from createOrderService through outbox cdc and consumer to history`() {
        val customerId = "customer-e2e-${UUID.randomUUID()}"
        val totalAmount = BigDecimal("149.99")

        val order = createOrderService.execute(
            PlaceOrderCommand(
                customerId = customerId,
                totalAmount = totalAmount
            )
        )

        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findAll().filter { it.orderId == order.id }
            assertThat(records).isNotEmpty()
            val record = records.first()
            assertThat(record.customerId).isEqualTo(customerId)
            assertThat(record.totalAmount).isEqualByComparingTo(totalAmount)
            assertThat(record.orderStatus).isEqualTo("CREATED")
        }
    }

    @Test
    fun `should consume valid plain JSON order created event without type headers and save history`() {
        val orderId = UUID.randomUUID()
        val customerId = "customer-123"
        val rawJsonPayload = """
            {
                "orderId": "$orderId",
                "customerId": "$customerId",
                "totalAmount": "99.99",
                "status": "CREATED"
            }
        """.trimIndent()

        val producerProps = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name
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
            assertThat(record.orderStatus).isEqualTo("CREATED")
        }
    }

    @Test
    fun `should route poison pill raw payload to DLT topic without serialization exceptions`() {
        val poisonPayload = "invalid-non-json-byte-payload".toByteArray(Charsets.UTF_8)
        val testKey = "poison-key-${UUID.randomUUID()}"

        val producerProps = mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to ByteArraySerializer::class.java.name
        )

        val dltConsumerProps = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "test-dlt-verifier-group-${UUID.randomUUID()}",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java.name,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to ByteArrayDeserializer::class.java.name
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
