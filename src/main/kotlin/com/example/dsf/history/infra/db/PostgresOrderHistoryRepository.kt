package com.example.dsf.history.infra.db

import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.domain.OrderHistoryRepository
import org.springframework.data.repository.CrudRepository
import java.util.UUID

interface PostgresOrderHistoryRepository :
    CrudRepository<OrderHistory, UUID>,
    OrderHistoryRepository
