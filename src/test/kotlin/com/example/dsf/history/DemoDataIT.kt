package com.example.dsf.history

import com.example.dsf.commons.OrderStatus
import com.example.dsf.history.application.queries.GetOrderHistoryQuery
import com.example.dsf.history.domain.OrderHistory
import com.example.dsf.utils.ClickhouseOrderHistoryTestRepository
import com.example.dsf.utils.IntegrationTestContainers
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * The demo orders of the scenario runbook are seeded into PostgreSQL only. They must reach the order history through
 * the regular pipeline (outbox -> Debezium -> Kafka -> consumer -> ClickHouse) with the timestamps the runbook uses.
 */
class DemoDataIT : IntegrationTestContainers() {
    @Autowired
    private lateinit var historyRepository: ClickhouseOrderHistoryTestRepository

    private val statusOrder = UUID.fromString("10000000-0000-4000-8000-000000000001")
    private val amountOrder = UUID.fromString("10000000-0000-4000-8000-000000000002")
    private val rebuildOrder = UUID.fromString("10000000-0000-4000-8000-000000000003")
    private val februaryOrder = UUID.fromString("10000000-0000-4000-8000-000000000004")

    @Test
    fun `should publish every demo order into the order history`() {
        awaitLatestState(statusOrder) { assertThat(it.orderStatus).isEqualTo(OrderStatus.COMPLETED) }
        awaitLatestState(amountOrder) {
            assertThat(it.orderStatus).isEqualTo(OrderStatus.PROCESSING)
            assertThat(it.totalAmount).isEqualByComparingTo("99.00")
        }
        awaitLatestState(rebuildOrder) { assertThat(it.orderStatus).isEqualTo(OrderStatus.PROCESSING) }
        awaitLatestState(februaryOrder) { assertThat(it.orderStatus).isEqualTo(OrderStatus.CREATED) }
    }

    @Test
    fun `should keep the seeded timeline for time travel`() {
        awaitLatestState(statusOrder) { assertThat(it.orderStatus).isEqualTo(OrderStatus.COMPLETED) }

        assertThat(findSnapshot(statusOrder, "2026-01-15T16:59:00Z")).isNull()
        assertThat(findSnapshot(statusOrder, "2026-01-15T17:02:00Z")?.orderStatus).isEqualTo(OrderStatus.CREATED)
        assertThat(findSnapshot(statusOrder, "2026-01-15T17:06:00Z")?.orderStatus).isEqualTo(OrderStatus.PROCESSING)
        assertThat(findSnapshot(statusOrder, "2026-01-15T17:11:00Z")?.orderStatus).isEqualTo(OrderStatus.COMPLETED)
    }

    @Test
    fun `should not return the February order in a January snapshot`() {
        awaitLatestState(februaryOrder) { assertThat(it.orderStatus).isEqualTo(OrderStatus.CREATED) }

        assertThat(findSnapshot(februaryOrder, "2026-01-31T23:59:00Z")).isNull()
    }

    // The history is written asynchronously, so the latest state is polled until it matches
    private fun awaitLatestState(
        orderId: UUID,
        assertion: (OrderHistory) -> Unit,
    ) {
        await.atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val records = historyRepository.findByOrderId(orderId)
            assertThat(records).hasSize(1)
            assertion(records.first())
        }
    }

    private fun findSnapshot(
        orderId: UUID,
        targetTime: String,
    ): OrderHistory? =
        historyRepository.findBy(GetOrderHistoryQuery(targetTime = Instant.parse(targetTime), orderId = orderId)).singleOrNull()
}
