package com.example.dsf.order.domain

import java.util.UUID

interface OrderRepository {
    fun findOrderById(orderId: UUID): Order?

    fun save(order: Order): Order

    fun findAll(): List<Order>
}
