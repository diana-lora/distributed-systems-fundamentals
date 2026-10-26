package com.example.dsf.history.application

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.commons.OrderStatusUpdatedEvent
import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.domain.OrderHistoryRepository
import com.example.dsf.history.infra.db.ClickhouseOrderHistoryRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.util.UUID

@Service
class ProcessOrderHistoryService(
    @Qualifier("postgresOrderHistoryRepository") private val orderHistoryRepository: OrderHistoryRepository,
    @Qualifier("clickhouseOrderHistoryRepository") private val clickhouseOrderHistoryRepository: ClickhouseOrderHistoryRepository,
    @Value($$"${app.toggle.order-history-source}") private val orderHistorySource: String,
) {
    @Transactional
    fun logOrderCreation(
        event: OrderCreatedEvent,
        version: Long,
    ) {
        val history =
            OrderHistory(
                orderId = UUID.fromString(event.orderId),
                customerId = event.customerId,
                totalAmount = event.totalAmount,
                orderStatus = event.status,
                version = version,
            )

        save(history)
    }

    @Transactional
    fun logOrderStatusUpdate(
        event: OrderStatusUpdatedEvent,
        version: Long,
    ) {
        val history =
            OrderHistory(
                orderId = UUID.fromString(event.orderId),
                customerId = event.customerId,
                totalAmount = event.totalAmount,
                orderStatus = event.newStatus,
                version = version,
            )

        save(history)
    }

    private fun save(history: OrderHistory) {
        when (orderHistorySource) {
            "postgres" -> orderHistoryRepository.save(history)
            "clickhouse" -> clickhouseOrderHistoryRepository.save(history)
            else -> throw IllegalArgumentException("Invalid order history source: $orderHistorySource")
        }
    }
}
