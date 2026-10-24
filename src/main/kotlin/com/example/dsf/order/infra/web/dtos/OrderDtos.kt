package com.example.dsf.order.infra.web.dtos

import com.example.dsf.order.domain.Order
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class CreateOrderRequest(
    val customerId: String,
    val totalAmount: BigDecimal
)

data class OrderResponse(
    val id: UUID,
    val customerId: String,
    val totalAmount: BigDecimal,
    val status: String,
    val createdAt: Instant
) {
    companion object {
        fun fromDomain(order: Order): OrderResponse = OrderResponse(
            id = order.id,
            customerId = order.customerId,
            totalAmount = order.totalAmount,
            status = order.status.name,
            createdAt = order.createdAt
        )
    }
}