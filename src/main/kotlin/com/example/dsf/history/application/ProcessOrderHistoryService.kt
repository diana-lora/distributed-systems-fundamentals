package com.example.dsf.history.application

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.domain.OrderHistoryRepository
import com.example.dsf.history.infra.db.ClickhouseOrderHistoryRepository
import java.math.BigDecimal
import java.util.UUID
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class ProcessOrderHistoryService(
    @Qualifier("postgresOrderHistoryRepository") private val orderHistoryRepository: OrderHistoryRepository,
    @Qualifier("clickhouseOrderHistoryRepository") private val clickhouseOrderHistoryRepository: ClickhouseOrderHistoryRepository,
    @Value($$"${app.toggle.order-history-source}") private val orderHistorySource: String,
) {
    @Transactional
    fun logOrderCreation(event: OrderCreatedEvent, version: Long) {
        val history = OrderHistory(
            orderId = UUID.fromString(event.orderId),
            customerId = event.customerId,
            totalAmount = BigDecimal(event.totalAmount),
            orderStatus = event.status,
            version = version
        )
        // FIXME: use enumeration instead of string
        when (orderHistorySource) {
            "postgres" -> orderHistoryRepository.save(history)
            "clickhouse" -> clickhouseOrderHistoryRepository.save(history)
            else -> throw IllegalArgumentException("Invalid order history source: $orderHistorySource")
        }
    }
}