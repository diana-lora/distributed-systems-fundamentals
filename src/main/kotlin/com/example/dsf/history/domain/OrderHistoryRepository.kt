package com.example.dsf.history.domain

import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

interface OrderHistoryRepository {
    fun save(history: OrderHistory): OrderHistory

    // Overwrites the amount in every version of the order, leaving status, updatedAt and version untouched
    fun updateTotalAmount(
        orderId: UUID,
        customerId: String,
        createdAt: Instant,
        newTotalAmount: BigDecimal,
    )
}
