package com.example.dsf.history

import com.example.dsf.config.KafkaConfig
import com.example.dsf.history.infra.db.SpringDataHistoryRepository
import com.example.dsf.order.application.CreateOrderService
import com.example.dsf.order.application.commands.PlaceOrderCommand
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import java.math.BigDecimal
import java.time.Duration
import java.util.UUID

@SpringBootTest
class OrderEventConsumerIntegrationTest {

    @Value($$"${spring.kafka.bootstrap-servers}")
    private lateinit var bootstrapServers: String

    @Autowired
    private lateinit var historyRepository: SpringDataHistoryRepository

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

        var found = false
        val deadline = System.currentTimeMillis() + 15000
        while (System.currentTimeMillis() < deadline) {
            val records = historyRepository.findAll().filter { it.orderId == order.id }
            if (records.isNotEmpty()) {
                val record = records.first()
                assertThat(record.customerId).isEqualTo(customerId)
                assertThat(record.totalAmount).isEqualByComparingTo(totalAmount)
                assertThat(record.orderStatus).isEqualTo("CREATED")
                found = true
                break
            }
            Thread.sleep(250)
        }

        assertThat(found).isTrue()
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
        var found = false
        val deadline = System.currentTimeMillis() + 10000
        while (System.currentTimeMillis() < deadline) {
            val records = historyRepository.findAll().filter { it.orderId == orderId }
            if (records.isNotEmpty()) {
                val record = records.first()
                assertThat(record.customerId).isEqualTo(customerId)
                assertThat(record.totalAmount).isEqualByComparingTo("99.99")
                assertThat(record.orderStatus).isEqualTo("CREATED")
                found = true
                break
            }
            Thread.sleep(200)
        }

        assertThat(found).isTrue()
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

            var dltMessageReceived = false
            val deadline = System.currentTimeMillis() + 15000
            while (System.currentTimeMillis() < deadline) {
                val records = dltConsumer.poll(Duration.ofMillis(500))
                for (rec in records) {
                    if (rec.key() == testKey) {
                        assertThat(rec.value()).isEqualTo(poisonPayload)
                        dltMessageReceived = true
                        break
                    }
                }
                if (dltMessageReceived) break
            }

            assertThat(dltMessageReceived).isTrue()
        }
    }
}
