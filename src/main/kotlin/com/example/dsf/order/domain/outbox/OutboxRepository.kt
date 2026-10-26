package com.example.dsf.order.domain.outbox

interface OutboxRepository {
    fun save(event: OutboxEvent): OutboxEvent
}
