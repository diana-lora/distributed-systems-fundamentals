package com.example.dsf.order.infra.db

import com.example.dsf.order.domain.outbox.OutboxEvent
import com.example.dsf.order.domain.outbox.OutboxRepository
import java.util.UUID
import org.springframework.data.repository.CrudRepository

interface SpringDataOutboxRepository : CrudRepository<OutboxEvent, UUID>,  OutboxRepository