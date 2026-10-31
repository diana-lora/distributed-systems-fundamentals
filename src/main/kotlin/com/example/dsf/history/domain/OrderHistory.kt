package com.example.dsf.history.domain

import com.example.dsf.commons.OrderStatus
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

data class OrderHistory(
    val orderId: UUID,
    val customerId: String,
    val totalAmount: BigDecimal,
    val orderStatus: OrderStatus,
    val createdAt: Instant,
    val updatedAt: Instant,
    val version: Long = 0L,
)
