package com.example.dsf.order.infra.db

import com.example.dsf.order.domain.outbox.OutboxEvent
import com.example.dsf.order.domain.outbox.OutboxRepository
import org.springframework.data.repository.CrudRepository
import java.util.UUID

interface PostgresOutboxRepository :
    CrudRepository<OutboxEvent, UUID>,
    OutboxRepository
