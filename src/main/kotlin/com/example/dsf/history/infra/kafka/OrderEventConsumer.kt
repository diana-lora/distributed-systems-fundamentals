package com.example.dsf.history.infra.kafka

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.config.KafkaConfig
import com.example.dsf.history.application.ProcessOrderHistoryService
import java.math.BigDecimal
import java.util.UUID
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class OrderEventConsumer(
    private val processOrderHistoryService: ProcessOrderHistoryService
) {
    private val log = LoggerFactory.getLogger(OrderEventConsumer::class.java)

    @KafkaListener(
        topics = [KafkaConfig.ORDER_EVENTS_TOPIC],
        groupId = "order-history-group"
    )
    fun consumeOrderCreatedEvent(event: OrderCreatedEvent) {
        log.info("Successfully received unpacked event payload from Kafka for Order ID: ${event.orderId}")

        try {
            processOrderHistoryService.logOrderCreation(
                orderId = UUID.fromString(event.orderId),
                customerId = event.customerId,
                totalAmount = BigDecimal(event.totalAmount),
                status = event.status
            )
        } catch (ex: Exception) {
            log.error("Failed to process order history recording from typed payload contract", ex)
        }
    }
}