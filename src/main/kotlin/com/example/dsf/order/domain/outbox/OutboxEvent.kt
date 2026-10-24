package com.example.dsf.order.domain.outbox

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Table
import java.time.Instant
import java.util.UUID
import org.springframework.data.domain.Persistable
import org.springframework.data.relational.core.mapping.Column

@Table("outbox")
data class OutboxEvent(
    @Id
    private val id: UUID = UUID.randomUUID(),
    @Column("aggregatetype") val aggregateType: String,
    @Column("aggregateid") val aggregateId: String,
    val aggregateKey: String,
    @Column("type") val eventType: String,
    val payload: String,
    val targetTopic: String,
    val createdAt: Instant = Instant.now()
): Persistable<UUID> {
    override fun getId(): UUID = id
    override fun isNew(): Boolean = true
}