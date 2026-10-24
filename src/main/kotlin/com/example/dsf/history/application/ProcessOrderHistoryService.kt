package com.example.dsf.history.application

import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.domain.OrderHistoryRepository
import java.math.BigDecimal
import java.util.UUID
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class ProcessOrderHistoryService(
    private val orderHistoryRepository: OrderHistoryRepository
) {
    @Transactional
    fun logOrderCreation(orderId: UUID, customerId: String, totalAmount: BigDecimal, status: String) {
        val historyEntry = OrderHistory(
            orderId = orderId,
            customerId = customerId,
            totalAmount = totalAmount,
            orderStatus = status,
            actionType = "CREATED"
        )
        orderHistoryRepository.save(historyEntry)
    }
}