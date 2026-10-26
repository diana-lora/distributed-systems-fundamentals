package com.example.dsf.order.application

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.commons.OrderEvent
import com.example.dsf.commons.OrderEventType.ORDER_CREATED
import com.example.dsf.config.KafkaConfig.Companion.ORDER_EVENTS_TOPIC
import com.example.dsf.order.application.commands.PlaceOrderCommand
import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.OrderRepository
import com.example.dsf.order.domain.outbox.OutboxEvent
import com.example.dsf.order.domain.outbox.OutboxRepository
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class CreateOrderService(
    private val orderRepository: OrderRepository,
    private val outboxRepository: OutboxRepository,
    private val objectMapper: ObjectMapper,
) {
    @Transactional
    fun execute(command: PlaceOrderCommand): Order {
        val order =
            Order(
                customerId = command.customerId,
                totalAmount = command.totalAmount,
            )
        val savedOrder = orderRepository.save(order)

        val orderCreatedEvent =
            OrderCreatedEvent(
                orderId = savedOrder.id.toString(),
                customerId = savedOrder.customerId,
                totalAmount = savedOrder.totalAmount,
                status = savedOrder.status,
                createdAt = savedOrder.createdAt,
                updatedAt = savedOrder.createdAt,
            )
        val jsonPayload = objectMapper.writerFor(OrderEvent::class.java).writeValueAsString(orderCreatedEvent)

        val outboxEvent =
            OutboxEvent(
                aggregateType = "Order",
                aggregateId = savedOrder.id.toString(),
                aggregateKey = savedOrder.customerId,
                eventType = ORDER_CREATED,
                payload = jsonPayload,
                targetTopic = ORDER_EVENTS_TOPIC,
            )
        outboxRepository.save(outboxEvent)

        return savedOrder
    }
}
