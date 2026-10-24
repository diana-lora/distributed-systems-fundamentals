package com.example.dsf.history.infra.db

import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.domain.OrderHistoryRepository
import java.util.UUID
import org.springframework.data.repository.CrudRepository

interface PostgresOrderHistoryRepository: CrudRepository<OrderHistory, UUID>, OrderHistoryRepository