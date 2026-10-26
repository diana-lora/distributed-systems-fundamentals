package com.example.dsf.history.infra.db

import com.example.dsf.commons.OrderStatus
import com.example.dsf.commons.toUtcInstant
import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.history.domain.OrderHistoryRepository
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.annotations.TestOnly
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Repository
import java.math.BigDecimal
import java.sql.ResultSet
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID
import javax.sql.DataSource
import kotlin.jvm.java

@Repository
open class ClickhouseOrderHistoryRepository(
    @Qualifier("clickhouseDataSource") private val datasource: DataSource,
) : OrderHistoryRepository {
    protected val log = KotlinLogging.logger {}

    override fun save(history: OrderHistory): OrderHistory {
        val sql =
            """
            INSERT INTO order_history (order_id, customer_id, total_amount, status, created_at, updated_at, version)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """.trimIndent()

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, history.orderId)
                stmt.setString(2, history.customerId)
                stmt.setBigDecimal(3, history.totalAmount)
                stmt.setString(4, history.orderStatus.name)
                // Instants are bound directly: the driver converts a LocalDateTime from the JVM time zone
                stmt.setObject(5, history.createdAt)
                stmt.setObject(6, history.updatedAt)
                stmt.setLong(7, history.version)
                stmt.executeUpdate()
            }
        }
        log.info {
            "Successfully appended domain record to ClickHouse for Order ID: ${history.orderId} with version offset: ${history.version}"
        }
        return history
    }

    /**
     * Retroactively corrects the total amount of an order in **all** its versions (lightweight update).
     *
     * **Why an update instead of an append:** every event carries the full order state, so a wrong amount is copied
     * into every version. An appended row would only replace one version in `argMax`, so earlier snapshots would keep
     * the wrong value. The lightweight `UPDATE` writes a small patch part that every read applies, so all snapshots
     * return the corrected amount while `status`, `updated_at` and `version` stay untouched.
     *
     * **How the patch is restricted to one partition:** all versions of an order share the same `created_at`, so the
     * table partition `toYYYYMM(created_at)` is computed from [createdAt]. The `created_at` range from the start of
     * that month (UTC) to the start of the next month lets ClickHouse skip every other partition, while
     * `customer_id` and `order_id` match the prefix of the sorting key.
     */
    override fun updateTotalAmount(
        orderId: UUID,
        customerId: String,
        createdAt: Instant,
        newTotalAmount: BigDecimal,
    ) {
        val sql =
            """
            UPDATE order_history
            SET total_amount = ?
            WHERE customer_id = ? AND order_id = ?
              AND created_at >= ? AND created_at < ?
            """.trimIndent()

        val partitionStart =
            createdAt
                .atZone(ZoneOffset.UTC)
                .toLocalDate()
                .withDayOfMonth(1)
                .atStartOfDay(ZoneOffset.UTC)
        val partitionEnd = partitionStart.plusMonths(1)

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setBigDecimal(1, newTotalAmount)
                stmt.setString(2, customerId)
                stmt.setObject(3, orderId)
                stmt.setObject(4, partitionStart.toInstant())
                stmt.setObject(5, partitionEnd.toInstant())
                stmt.executeUpdate()
            }
        }
        log.info { "Successfully patched total amount in ClickHouse for Order ID: $orderId to $newTotalAmount" }
    }

    /**
     * Reconstructs the state of every order exactly as it was at [targetTime] (time travel).
     *
     * **How the latest version is calculated:** each status change is appended as a new row, so one order owns
     * several rows that differ only by `version` (the Kafka offset of the event). Instead of the expensive `FINAL`
     * modifier, the rows are collapsed in memory with `GROUP BY order_id`: `argMax(column, version)` returns the
     * value of the row with the highest version, i.e. the most recent state. `created_at` is the same on every row
     * of an order, so `any(created_at)` is enough, while `max(updated_at)` returns the time of the latest change.
     *
     * **How the search is restricted to partitions:** the table is partitioned by `toYYYYMM(created_at)`. An order
     * created after [targetTime] cannot be part of the snapshot, so the `created_at <= ?` filter lets ClickHouse
     * skip all monthly partitions newer than [targetTime] without reading them. Inside the remaining partitions,
     * `updated_at <= ?` drops the versions that happened after [targetTime] before the aggregation runs.
     */
    fun findBefore(targetTime: java.time.Instant): List<OrderHistory> {
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
            WHERE created_at <= ? AND updated_at <= ?
            GROUP BY order_id
            """.trimIndent()

        val records = mutableListOf<OrderHistory>()

        datasource.connection.use { connection ->
            connection.prepareStatement(sql).use { stmt ->
                stmt.setObject(1, targetTime)
                stmt.setObject(2, targetTime)

                stmt.executeQuery().use { resultSet ->
                    while (resultSet.next()) {
                        records.add(resultSet.toOrderHistory())
                    }
                }
            }
        }
        log.info { "Time travel execution completed for timestamp: $targetTime. Found ${records.size} historical states." }
        return records
    }

    protected fun ResultSet.toOrderHistory(): OrderHistory =
        OrderHistory(
            orderId = getObject("order_id", java.util.UUID::class.java),
            customerId = getString("latest_customer_id"),
            totalAmount = getBigDecimal("latest_total_amount"),
            orderStatus = OrderStatus.valueOf(getString("latest_status")),
            createdAt = getObject("latest_created_at", LocalDateTime::class.java).toUtcInstant(),
            updatedAt = getObject("latest_updated_at", LocalDateTime::class.java).toUtcInstant(),
            version = getLong("latest_version"),
        )
}
