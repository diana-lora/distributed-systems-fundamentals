package com.example.dsf.order.domain

import java.util.UUID

interface OrderStatusChangeRepository {
    fun save(change: OrderStatusChange): OrderStatusChange

    fun findAllByOrderIdOrderByCreatedAtAsc(orderId: UUID): List<OrderStatusChange>
}
