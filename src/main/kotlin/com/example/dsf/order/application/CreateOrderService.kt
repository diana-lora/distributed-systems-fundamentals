package com.example.dsf.order.application

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.order.application.commands.PlaceOrderCommand
import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.OrderRepository
import com.example.dsf.order.domain.OrderStatusChange
import com.example.dsf.order.domain.OrderStatusChangeRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class CreateOrderService(
    private val orderRepository: OrderRepository,
    private val orderStatusChangeRepository: OrderStatusChangeRepository,
    private val outboxEventService: OutboxEventService,
) {
    @Transactional
    fun execute(command: PlaceOrderCommand): Order {
        val order =
            Order(
                customerId = command.customerId,
                totalAmount = command.totalAmount,
            )
        val savedOrder = orderRepository.save(order)

        orderStatusChangeRepository.save(
            OrderStatusChange(orderId = savedOrder.id, status = savedOrder.status, createdAt = savedOrder.createdAt),
        )

        val orderCreatedEvent =
            OrderCreatedEvent(
                orderId = savedOrder.id.toString(),
                customerId = savedOrder.customerId,
                totalAmount = savedOrder.totalAmount,
                status = savedOrder.status,
                createdAt = savedOrder.createdAt,
                updatedAt = savedOrder.createdAt,
            )
        outboxEventService.save(savedOrder, orderCreatedEvent)

        return savedOrder
    }
}
