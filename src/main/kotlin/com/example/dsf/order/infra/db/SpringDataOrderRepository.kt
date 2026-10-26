package com.example.dsf.order.infra.db

import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.OrderRepository
import org.springframework.data.repository.CrudRepository
import java.util.UUID

interface SpringDataOrderRepository :
    CrudRepository<Order, UUID>,
    OrderRepository
