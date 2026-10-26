package com.example.dsf.utils

import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.infra.db.ClickhouseOrderHistoryRepository
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Repository
import java.sql.PreparedStatement
import java.util.UUID
import javax.sql.DataSource

@Repository
class ClickhouseOrderHistoryTestRepository(
    @Qualifier("clickhouseDataSource") private val datasource: DataSource,
) : ClickhouseOrderHistoryRepository(datasource) {
    fun findAll(): List<OrderHistory> = findLatestStates(whereClause = "")

    // Returns the latest state of the order (one consolidated row), not every stored version
    fun findByOrderId(orderId: UUID): List<OrderHistory> =
        findLatestStates(whereClause = "WHERE order_id = ?") { stmt -> stmt.setObject(1, orderId) }

    /** Forces ClickHouse to merge all parts immediately, as a background merge would do eventually. */
    fun optimizeFinal() {
        datasource.connection.use { connection ->
            connection.createStatement().use { stmt -> stmt.execute("OPTIMIZE TABLE order_history FINAL") }
        }
    }

    private fun findLatestStates(
        whereClause: String,
        bind: (PreparedStatement) -> Unit = {},
    ): List<OrderHistory> {
        val sql =
            """
            SELECT
                order_id,
                argMax(customer_id, version) AS latest_customer_id,
                argMax(total_amount, version) AS latest_total_amount,
                argMax(status, version) AS latest_status,
                any(created_at) AS latest_created_at,
                max(updated_at) AS latest_updated_at,
                max(version) AS latest_version
            FROM order_history
            $whereClause
            GROUP BY order_id
            """.trimIndent()
        val records = mutableListOf<OrderHistory>()

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                bind(stmt)
                stmt.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        records.add(resultSet.toOrderHistory())
                    }
                }
            }
        }
        log.info { "Successfully fetched ${records.size} consolidated records from ClickHouse using argMax aggregation" }
        return records
    }
}
