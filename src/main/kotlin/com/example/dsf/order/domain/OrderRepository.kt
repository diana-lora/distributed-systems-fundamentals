package com.example.dsf.order.domain

import java.util.Optional
import java.util.UUID

interface OrderRepository {
    fun findById(orderId: UUID): Optional<Order>
    fun save(order: Order): Order
    fun findAll(): List<Order>
}