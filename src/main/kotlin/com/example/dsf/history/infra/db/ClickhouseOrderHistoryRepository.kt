package com.example.dsf.history.infra.db

import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.domain.OrderHistoryRepository
import java.sql.ResultSet
import java.time.LocalDateTime
import java.time.ZoneOffset
import javax.sql.DataSource
import kotlin.jvm.java
import org.jetbrains.annotations.TestOnly
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Repository

@Repository
class ClickhouseOrderHistoryRepository(
    @Qualifier("clickhouseDataSource") private val datasource: DataSource
) : OrderHistoryRepository {

    private val log = LoggerFactory.getLogger(ClickhouseOrderHistoryRepository::class.java)

    override fun save(history: OrderHistory): OrderHistory {
        val sql = """
            INSERT INTO order_history (order_id, customer_id, total_amount, status, created_at, version)
            VALUES (?, ?, ?, ?, ?, ?)
        """.trimIndent()

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, history.orderId)
                stmt.setString(2, history.customerId)
                stmt.setBigDecimal(3, history.totalAmount)
                stmt.setString(4, history.orderStatus)
                stmt.setObject(5, history.processedAt.atZone(ZoneOffset.UTC).toLocalDateTime())
                stmt.setLong(6, history.version)
                stmt.executeUpdate()
            }
        }
        log.info(
            "Successfully appended domain record to ClickHouse for Order ID: {} with version offset: {}",
            history.orderId,
            history.version
        )
        return history
    }

    fun findBefore(targetTime: java.time.Instant): List<OrderHistory> {
        val sql = """
            SELECT 
                order_id,
                argMax(customer_id, version) AS latest_customer_id,
                argMax(total_amount, version) AS latest_total_amount,
                argMax(status, version) AS latest_status,
                max(created_at) AS latest_created_at,
                max(version) AS latest_version
            FROM order_history 
            WHERE created_at <= ?
            GROUP BY order_id
        """.trimIndent()

        val records = mutableListOf<OrderHistory>()

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, LocalDateTime.ofInstant(targetTime, ZoneOffset.UTC))

                stmt.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        records.add(resultSet.toOrderHistory())
                    }
                }
            }
        }
        log.info("Time travel execution completed for timestamp: {}. Found {} historical states.", targetTime, records.size)
        return records
    }

    @TestOnly
    fun findAll(): List<OrderHistory> {
        val sql = """
            SELECT 
                order_id,
                argMax(customer_id, version) AS latest_customer_id,
                argMax(total_amount, version) AS latest_total_amount,
                argMax(status, version) AS latest_status,
                max(created_at) AS latest_created_at,
                max(version) AS latest_version
            FROM order_history 
            GROUP BY order_id
        """.trimIndent()
        val records = mutableListOf<OrderHistory>()

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        records.add(resultSet.toOrderHistory())
                    }
                }
            }
        }
        log.info("Successfully fetched {} consolidated records from ClickHouse using FINAL execution modifier", records.size)
        return records
    }

    private fun ResultSet.toOrderHistory(): OrderHistory {
        return OrderHistory(
            orderId = getObject("order_id", java.util.UUID::class.java),
            customerId = getString("latest_customer_id"),
            totalAmount = getBigDecimal("latest_total_amount"),
            orderStatus = getString("latest_status"),
            processedAt = getTimestamp("latest_created_at").toInstant(),
            version = getLong("latest_version")
        )
    }
}