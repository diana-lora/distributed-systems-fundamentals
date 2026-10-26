package com.example.dsf.order.infra.web.dtos

import com.example.dsf.commons.OrderStatus
import com.example.dsf.order.domain.Order
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class CreateOrderRequest(
    val customerId: String,
    val totalAmount: BigDecimal,
)

data class OrderResponse(
    val id: UUID,
    val customerId: String,
    val totalAmount: BigDecimal,
    val status: OrderStatus,
    val createdAt: Instant,
) {
    companion object {
        fun fromDomain(order: Order): OrderResponse =
            OrderResponse(
                id = order.id,
                customerId = order.customerId,
                totalAmount = order.totalAmount,
                status = order.status,
                createdAt = order.createdAt,
            )
    }
}

data class UpdateOrderStatusRequest(
    val orderId: UUID,
    val customerId: String,
    val status: OrderStatus,
)
