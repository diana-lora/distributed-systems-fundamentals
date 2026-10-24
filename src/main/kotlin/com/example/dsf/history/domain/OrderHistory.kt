package com.example.dsf.history.domain

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID
import org.springframework.data.domain.Persistable

@Table("order_history")
data class OrderHistory(
    @Id
    private val id: UUID = UUID.randomUUID(),
    val orderId: UUID,
    val customerId: String,
    val totalAmount: BigDecimal,
    val orderStatus: String,
    val actionType: String,               // e.g., "CREATED", "UPDATED"
    val processedAt: Instant = Instant.now()
): Persistable<UUID> {
    override fun getId(): UUID = id
    override fun isNew(): Boolean = true
}