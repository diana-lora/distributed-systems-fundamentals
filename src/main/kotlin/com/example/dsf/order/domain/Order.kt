package com.example.dsf.order.domain

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID
import org.springframework.data.domain.Persistable

@Table("orders")
class Order(
    @Id
    private val id: UUID = UUID.randomUUID(),
    val customerId: String,
    val totalAmount: BigDecimal,
    val status: OrderStatus = OrderStatus.CREATED,
    val createdAt: Instant = Instant.now()
) : Persistable<UUID> {
    override fun getId(): UUID = id
    override fun isNew(): Boolean = true
}

enum class OrderStatus {
    CREATED, PROCESSING, COMPLETED, CANCELLED
}