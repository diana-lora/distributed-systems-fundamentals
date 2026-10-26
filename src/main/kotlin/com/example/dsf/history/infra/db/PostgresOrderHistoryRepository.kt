package com.example.dsf.history.infra.db

import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.domain.OrderHistoryRepository
import org.springframework.data.jdbc.repository.query.Modifying
import org.springframework.data.jdbc.repository.query.Query
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.query.Param
import java.math.BigDecimal
import java.time.Instant
import java.util.UUID

interface PostgresOrderHistoryRepository :
    CrudRepository<OrderHistory, UUID>,
    OrderHistoryRepository {
    // createdAt is only needed by ClickHouse for partition pruning
    @Modifying
    @Query(
        """
        UPDATE order_history SET total_amount = :newTotalAmount
        WHERE order_id = :orderId AND customer_id = :customerId
        """,
    )
    override fun updateTotalAmount(
        @Param("orderId") orderId: UUID,
        @Param("customerId") customerId: String,
        @Param("createdAt") createdAt: Instant,
        @Param("newTotalAmount") newTotalAmount: BigDecimal,
    )
}
