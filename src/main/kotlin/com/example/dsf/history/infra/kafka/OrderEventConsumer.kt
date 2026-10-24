package com.example.dsf.history.infra.kafka

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.config.KafkaConfig
import com.example.dsf.history.application.ProcessOrderHistoryService
import com.example.dsf.history.domain.OrderHistory
import java.math.BigDecimal
import java.util.UUID
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component
class OrderEventConsumer(
    private val processOrderHistoryService: ProcessOrderHistoryService
) {
    private val log = LoggerFactory.getLogger(OrderEventConsumer::class.java)

    @KafkaListener(
        topics = [KafkaConfig.ORDER_EVENTS_TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}"
    )
    fun consumeOrderCreatedEvent(
        event: OrderCreatedEvent,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        log.info("Successfully received unpacked event payload from Kafka for Order ID: ${event.orderId}")

        try {
            processOrderHistoryService.logOrderCreation(event = event, version = offset)
        } catch (ex: Exception) {
            log.error("Failed to process order history recording from typed payload contract", ex)
            throw ex
        }
    }
}