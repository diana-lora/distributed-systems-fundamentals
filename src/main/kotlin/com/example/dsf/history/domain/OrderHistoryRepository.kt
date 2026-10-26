package com.example.dsf.history.domain

interface OrderHistoryRepository {
    fun save(history: OrderHistory): OrderHistory
}
