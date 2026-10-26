package com.example.dsf.history.domain

import com.example.dsf.commons.OrderStatus
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID
import org.springframework.data.annotation.Id
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Table

@Table("order_history")
data class OrderHistory(
    @Id
    private val id: UUID = UUID.randomUUID(),
    val orderId: UUID,
    val customerId: String,
    val totalAmount: BigDecimal,
    val orderStatus: OrderStatus,
    val processedAt: Instant = Instant.now(),
    val version: Long = 0L
): Persistable<UUID> {
    override fun getId(): UUID = id
    override fun isNew(): Boolean = true
}