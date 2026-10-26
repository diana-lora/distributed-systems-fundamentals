package com.example.dsf.history.infra.kafka

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.commons.OrderEventEnvelope
import com.example.dsf.commons.OrderStatusUpdatedEvent
import com.example.dsf.config.KafkaConfig
import com.example.dsf.history.application.ProcessOrderHistoryService
import com.fasterxml.jackson.databind.ObjectMapper
import kotlin.jvm.java
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.stereotype.Component

@Component
class OrderEventConsumer(
    private val processOrderHistoryService: ProcessOrderHistoryService,
    private val objectMapper: ObjectMapper
) {
    private val log = LoggerFactory.getLogger(OrderEventConsumer::class.java)

    @KafkaListener(
        topics = [KafkaConfig.ORDER_EVENTS_TOPIC],
        groupId = "\${spring.kafka.consumer.group-id}"
    )
    fun consumeOrderCreatedEvent(
        message: String,
        @Header(KafkaHeaders.OFFSET) offset: Long
    ) {
        val event = objectMapper.readValue(message, OrderEventEnvelope::class.java)

        try {
            when(event.payload) {
                is OrderCreatedEvent -> {
                    val orderEvent = event.payload
                    log.info("Creating event for Order ID: ${orderEvent.orderId}")
                    processOrderHistoryService.logOrderCreation(event = event.payload, version = offset)
                }
                is OrderStatusUpdatedEvent -> {
                    val orderEvent = event.payload
                    log.info("Updating event's status for Order ID: ${orderEvent.orderId} to ${orderEvent.newStatus}")
                    processOrderHistoryService.logOrderStatusUpdate(event = orderEvent, version = offset)
                }
            }
        } catch (ex: Exception) {
            log.error("Failed to process order history recording from typed payload contract", ex)
            throw ex
        }
    }
}