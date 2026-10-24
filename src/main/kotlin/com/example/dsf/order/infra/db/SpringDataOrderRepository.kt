package com.example.dsf.order.infra.db

import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.OrderRepository
import java.util.UUID
import org.springframework.data.repository.CrudRepository

interface SpringDataOrderRepository : CrudRepository<Order, UUID>, OrderRepository