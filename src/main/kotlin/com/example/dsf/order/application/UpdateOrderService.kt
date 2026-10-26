package com.example.dsf.order.application

import com.example.dsf.commons.OrderEvent
import com.example.dsf.commons.OrderEventType.ORDER_STATUS_UPDATED
import com.example.dsf.commons.OrderEventType.ORDER_TOTAL_AMOUNT_UPDATED
import com.example.dsf.commons.OrderStatusUpdatedEvent
import com.example.dsf.commons.OrderTotalAmountUpdateEvent
import com.example.dsf.config.KafkaConfig.Companion.ORDER_EVENTS_TOPIC
import com.example.dsf.order.application.commands.UpdateOrderStatusCommand
import com.example.dsf.order.application.commands.UpdateOrderTotalAmountCommand
import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.OrderRepository
import com.example.dsf.order.domain.exceptions.OrderNotFoundException
import com.example.dsf.order.domain.outbox.OutboxEvent
import com.example.dsf.order.domain.outbox.OutboxRepository
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

@Service
class UpdateOrderService(
    private val orderRepository: OrderRepository,
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper,
) {
    @Transactional
    fun execute(command: UpdateOrderStatusCommand): Order {
        val order = findOrder(command.orderId)

        order.status = command.newStatus
        val savedOrder = orderRepository.save(order)

        val updatedEvent =
            OrderStatusUpdatedEvent(
                orderId = savedOrder.id.toString(),
                customerId = savedOrder.customerId,
                totalAmount = savedOrder.totalAmount,
                newStatus = savedOrder.status,
                createdAt = savedOrder.createdAt,
                updatedAt = Instant.now(),
            )
        saveToOutbox(savedOrder, ORDER_STATUS_UPDATED, updatedEvent)

        return savedOrder
    }

    @Transactional
    fun execute(command: UpdateOrderTotalAmountCommand): Order {
        val order = findOrder(command.orderId)

        order.totalAmount = command.newTotalAmount
        val savedOrder = orderRepository.save(order)

        val updatedEvent =
            OrderTotalAmountUpdateEvent(
                orderId = savedOrder.id.toString(),
                customerId = savedOrder.customerId,
                newTotalAmount = savedOrder.totalAmount,
                createdAt = savedOrder.createdAt,
            )
        saveToOutbox(savedOrder, ORDER_TOTAL_AMOUNT_UPDATED, updatedEvent)

        return savedOrder
    }

    private fun findOrder(orderId: UUID): Order =
        orderRepository
            .findById(orderId)
            .orElseThrow { OrderNotFoundException("Order not found (ID=$orderId)") }

    private fun saveToOutbox(
        order: Order,
        eventType: String,
        event: OrderEvent,
    ) {
        val jsonPayload = objectMapper.writerFor(OrderEvent::class.java).writeValueAsString(event)

        val outboxEvent =
            OutboxEvent(
                aggregateType = "Order",
                aggregateId = order.id.toString(),
                aggregateKey = order.customerId,
                eventType = eventType,
                payload = jsonPayload,
                targetTopic = ORDER_EVENTS_TOPIC,
            )
        outboxRepository.save(outboxEvent)
    }
}
