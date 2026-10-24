package com.example.dsf.order.application

import com.example.dsf.commons.OrderCreatedEvent
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
    private val objectMapper: ObjectMapper
) {

    @Transactional
    fun execute(command: PlaceOrderCommand): Order {
        // 1. Create and persist the core Business Entity
        val order = Order(
            customerId = command.customerId,
            totalAmount = command.totalAmount
        )
        val savedOrder = orderRepository.save(order)

        // 2. Serialize the event payload data structure
        val orderCreatedEvent = OrderCreatedEvent(
            orderId = savedOrder.id.toString(),
            customerId = savedOrder.customerId,
            totalAmount = savedOrder.totalAmount.toString(),
            status = savedOrder.status.name
        )
        val jsonPayload = objectMapper.writeValueAsString(orderCreatedEvent)

        // 3. Create and persist the Transactional Outbox Log Entry
        val outboxEvent = OutboxEvent(
            aggregateType = "Order",
            aggregateId = savedOrder.id.toString(),
            aggregateKey = savedOrder.customerId, // Routes all events belonging to this customer to the same Kafka partition
            eventType = "ORDER_CREATED",
            payload = jsonPayload,
            targetTopic = "order-events" // Dynamic topic routing parameter for Debezium
        )
        outboxRepository.save(outboxEvent)

        return savedOrder
    }
}
