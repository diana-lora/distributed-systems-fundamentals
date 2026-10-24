package com.example.dsf.order.domain

interface OrderRepository {
    fun save(order: Order): Order
    fun findAll(): List<Order>
}