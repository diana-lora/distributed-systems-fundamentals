package com.example.dsf.order.application

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.commons.OrderEvent
import com.example.dsf.commons.OrderEventType.ORDER_CREATED
import com.example.dsf.commons.OrderEventType.ORDER_REPAIR
import com.example.dsf.commons.OrderEventType.ORDER_STATUS_UPDATED
import com.example.dsf.commons.OrderEventType.ORDER_TOTAL_AMOUNT_UPDATED
import com.example.dsf.commons.OrderRepairEvent
import com.example.dsf.commons.OrderStatusUpdatedEvent
import com.example.dsf.commons.OrderTotalAmountUpdateEvent
import com.example.dsf.config.KafkaConfig.Companion.ORDER_EVENTS_TOPIC
import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.outbox.OutboxEvent
import com.example.dsf.order.domain.outbox.OutboxRepository
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional

@Service
class OutboxEventService(
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper,
) {
    // Mandatory: the event must be committed in the same transaction as the order change it describes
    @Transactional(propagation = Propagation.MANDATORY)
    fun save(
        order: Order,
        event: OrderEvent,
    ): OutboxEvent {
        val jsonPayload = objectMapper.writerFor(OrderEvent::class.java).writeValueAsString(event)

        val outboxEvent =
            OutboxEvent(
                aggregateType = "Order",
                aggregateId = order.id.toString(),
                aggregateKey = order.customerId,
                eventType = eventTypeOf(event),
                payload = jsonPayload,
                targetTopic = ORDER_EVENTS_TOPIC,
            )
        return outboxRepository.save(outboxEvent)
    }

    private fun eventTypeOf(event: OrderEvent): String =
        when (event) {
            is OrderCreatedEvent -> ORDER_CREATED
            is OrderStatusUpdatedEvent -> ORDER_STATUS_UPDATED
            is OrderTotalAmountUpdateEvent -> ORDER_TOTAL_AMOUNT_UPDATED
            is OrderRepairEvent -> ORDER_REPAIR
        }
}
