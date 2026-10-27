package com.example.dsf.history

import com.example.dsf.commons.OrderStatus
import com.example.dsf.history.infra.web.dtos.OrderHistoryResponse
import com.example.dsf.order.infra.web.dtos.CreateOrderRequest
import com.example.dsf.order.infra.web.dtos.OrderResponse
import com.example.dsf.order.infra.web.dtos.UpdateOrderStatusRequest
import com.example.dsf.order.infra.web.dtos.UpdateOrderTotalAmountRequest
import com.example.dsf.utils.IntegrationTestContainers
import com.fasterxml.jackson.module.kotlin.readValue
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.await
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.put
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * Covers the time travel scenarios of ADR 0003 end to end through the public API: orders are changed with the order
 * endpoints, the history is written asynchronously (outbox -> Debezium -> Kafka -> consumer -> ClickHouse) and read
 * back with `GET /ordersHistory`.
 */
class OrderHistoryTimeTravelIT : IntegrationTestContainers() {
    @Test
    fun `order status update returns the status valid at each point in time`() {
        val customerId = "customer-status-${UUID.randomUUID()}"
        val orderId = createOrder(customerId, BigDecimal("49.90"))
        val createdTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.CREATED) }.updatedAt

        // Some time later the order moves on
        Thread.sleep(50)
        updateStatus(orderId, customerId, OrderStatus.PROCESSING)
        val processingTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.PROCESSING) }.updatedAt

        assertThat(processingTime).isAfter(createdTime)
        assertThat(getOrderHistory(orderId, createdTime.minusMillis(1))).isEmpty()
        assertThat(getSnapshot(orderId, createdTime).status).isEqualTo(OrderStatus.CREATED)
        assertThat(getSnapshot(orderId, processingTime).status).isEqualTo(OrderStatus.PROCESSING)
        assertThat(getOrderHistory(orderId).single().status).isEqualTo(OrderStatus.PROCESSING)
    }

    @Test
    fun `retroactive total amount correction applies to every past snapshot`() {
        val customerId = "customer-amount-${UUID.randomUUID()}"
        val wrongAmount = BigDecimal("99.00")
        val correctAmount = BigDecimal("9.90")
        val orderId = createOrder(customerId, wrongAmount)
        val createdTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.CREATED) }.updatedAt

        Thread.sleep(50)
        updateStatus(orderId, customerId, OrderStatus.PROCESSING)
        val processingTime = awaitLatestState(orderId) { assertThat(it.status).isEqualTo(OrderStatus.PROCESSING) }.updatedAt

        // The typo was copied into every version of the order
        assertThat(getSnapshot(orderId, createdTime).totalAmount).isEqualByComparingTo(wrongAmount)
        assertThat(getSnapshot(orderId, processingTime).totalAmount).isEqualByComparingTo(wrongAmount)

        updateTotalAmount(orderId, customerId, correctAmount)
        awaitLatestState(orderId) { assertThat(it.totalAmount).isEqualByComparingTo(correctAmount) }

        val createdSnapshot = getSnapshot(orderId, createdTime)
        assertThat(createdSnapshot.status).isEqualTo(OrderStatus.CREATED)
        assertThat(createdSnapshot.totalAmount).isEqualByComparingTo(correctAmount)
        assertThat(createdSnapshot.updatedAt).isEqualTo(createdTime)

        val processingSnapshot = getSnapshot(orderId, processingTime)
        assertThat(processingSnapshot.status).isEqualTo(OrderStatus.PROCESSING)
        assertThat(processingSnapshot.totalAmount).isEqualByComparingTo(correctAmount)
        assertThat(processingSnapshot.updatedAt).isEqualTo(processingTime)
    }

    @Test
    @Disabled("History rebuild with backdated versions is not implemented yet")
    fun `history rebuild with backdated versions repairs the corrupted state from the moment it happened`() {
        // 1. Create an order and wait for the CREATED snapshot.
        // 2. Store a corrupted status version (e.g. COMPLETED instead of PROCESSING).
        // 3. Trigger the history rebuild, which inserts the repaired state backdated to the corrupted updatedAt.
        // 4. Assert that a snapshot between the corruption and the rebuild already returns the repaired status.
    }

    private fun createOrder(
        customerId: String,
        totalAmount: BigDecimal,
    ): UUID {
        val response =
            mockMvc
                .post("/orders") {
                    contentType = MediaType.APPLICATION_JSON
                    content = objectMapper.writeValueAsString(CreateOrderRequest(customerId, totalAmount))
                }.andExpect { status { isCreated() } }
                .andReturn()
                .response.contentAsString
        return objectMapper.readValue<OrderResponse>(response).id
    }

    private fun updateStatus(
        orderId: UUID,
        customerId: String,
        status: OrderStatus,
    ) {
        mockMvc
            .put("/orders/status") {
                contentType = MediaType.APPLICATION_JSON
                content = objectMapper.writeValueAsString(UpdateOrderStatusRequest(orderId, customerId, status))
            }.andExpect { status { isOk() } }
    }

    private fun updateTotalAmount(
        orderId: UUID,
        customerId: String,
        totalAmount: BigDecimal,
    ) {
        mockMvc
            .put("/orders/totalAmount") {
                contentType = MediaType.APPLICATION_JSON
                content = objectMapper.writeValueAsString(UpdateOrderTotalAmountRequest(orderId, customerId, totalAmount))
            }.andExpect { status { isOk() } }
    }

    // Without targetTime the endpoint uses its default (now)
    private fun getOrderHistory(
        orderId: UUID,
        targetTime: Instant? = null,
    ): List<OrderHistoryResponse> {
        val response =
            mockMvc
                .get("/ordersHistory") {
                    param("orderId", orderId.toString())
                    targetTime?.let { param("targetTime", it.toString()) }
                }.andExpect { status { isOk() } }
                .andReturn()
                .response.contentAsString
        return objectMapper.readValue(response)
    }

    private fun getSnapshot(
        orderId: UUID,
        targetTime: Instant,
    ): OrderHistoryResponse = getOrderHistory(orderId, targetTime).single()

    // The history is written asynchronously, so the latest state is polled until it matches
    private fun awaitLatestState(
        orderId: UUID,
        assertion: (OrderHistoryResponse) -> Unit,
    ): OrderHistoryResponse {
        lateinit var latest: OrderHistoryResponse
        await.atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(250)).untilAsserted {
            val snapshots = getOrderHistory(orderId)
            assertThat(snapshots).hasSize(1)
            latest = snapshots.first()
            assertion(latest)
        }
        return latest
    }
}
