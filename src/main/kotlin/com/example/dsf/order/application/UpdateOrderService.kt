package com.example.dsf.order.application

import com.example.dsf.commons.OrderEventEnvelope
import com.example.dsf.commons.OrderEventType.ORDER_STATUS_UPDATED
import com.example.dsf.commons.OrderStatusUpdatedEvent
import com.example.dsf.config.KafkaConfig.Companion.ORDER_EVENTS_TOPIC
import com.example.dsf.order.application.commands.UpdateOrderCommand
import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.exceptions.OrderNotFoundException
import com.example.dsf.order.domain.OrderRepository
import com.example.dsf.order.domain.outbox.OutboxEvent
import com.example.dsf.order.domain.outbox.OutboxRepository
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class UpdateOrderService(
    private val orderRepository: OrderRepository,
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper
) {

    @Transactional
    fun execute(command: UpdateOrderCommand): Order {
        val order = orderRepository.findById(command.orderId)
            .orElseThrow { OrderNotFoundException("Order not found (ID=${command.orderId})") }

        order.status = command.newStatus
        val savedOrder = orderRepository.save(order)

        val updatedEvent = OrderStatusUpdatedEvent(
            orderId = savedOrder.id.toString(),
            customerId = savedOrder.customerId,
            totalAmount = savedOrder.totalAmount,
            newStatus = savedOrder.status
        )
        val envelope = OrderEventEnvelope(type = ORDER_STATUS_UPDATED, payload = updatedEvent)
        val jsonPayload = objectMapper.writeValueAsString(envelope)

        val outboxEvent = OutboxEvent(
            aggregateType = "Order",
            aggregateId = savedOrder.id.toString(),
            aggregateKey = savedOrder.customerId,
            eventType = ORDER_STATUS_UPDATED,
            payload = jsonPayload,
            targetTopic = ORDER_EVENTS_TOPIC
        )
        outboxRepository.save(outboxEvent)

        return savedOrder
    }
}