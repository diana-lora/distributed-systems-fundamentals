package com.example.dsf.order.infra.db

import com.example.dsf.order.domain.OrderStatusChange
import com.example.dsf.order.domain.OrderStatusChangeRepository
import org.springframework.data.repository.CrudRepository
import java.util.UUID

interface PostgresOrderStatusChangeRepository :
    CrudRepository<OrderStatusChange, UUID>,
    OrderStatusChangeRepository
