package com.example.dsf.order.application

import com.example.dsf.commons.OrderCreatedEvent
import com.example.dsf.commons.OrderEvent
import com.example.dsf.commons.OrderEventType.ORDER_CREATED
import com.example.dsf.commons.OrderStatus
import com.example.dsf.config.KafkaConfig
import com.example.dsf.config.KafkaConfig.Companion.ORDER_EVENTS_TOPIC
import com.example.dsf.order.application.commands.PlaceOrderCommand
import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.OrderRepository
import com.example.dsf.order.domain.OrderStatusChange
import com.example.dsf.order.domain.OrderStatusChangeRepository
import com.example.dsf.order.domain.outbox.OutboxEvent
import com.example.dsf.order.domain.outbox.OutboxRepository
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class CreateOrderServiceTest {
    private val orderRepository = mockk<OrderRepository>()
    private val orderStatusChangeRepository = mockk<OrderStatusChangeRepository>()
    private val outboxRepository = mockk<OutboxRepository>()
    private val objectMapper = KafkaConfig("unused").customObjectMapper()
    private val service =
        CreateOrderService(orderRepository, orderStatusChangeRepository, OutboxEventService(outboxRepository, objectMapper))

    @Test
    fun `should save the order, its CREATED status change and an ORDER_CREATED outbox event`() {
        every { orderRepository.save(any()) } answers { firstArg() }
        val statusChange = slot<OrderStatusChange>()
        every { orderStatusChangeRepository.save(capture(statusChange)) } answers { firstArg() }
        val outboxEvent = slot<OutboxEvent>()
        every { outboxRepository.save(capture(outboxEvent)) } answers { firstArg() }

        val order = service.execute(PlaceOrderCommand(customerId = "customer-1", totalAmount = BigDecimal("49.90")))

        assertThat(order.customerId).isEqualTo("customer-1")
        assertThat(order.totalAmount).isEqualByComparingTo("49.90")
        assertThat(order.status).isEqualTo(OrderStatus.CREATED)
        // Millisecond precision, so PostgreSQL and ClickHouse DateTime64(3) store the same value
        assertThat(order.createdAt.nano % 1_000_000).isZero()

        with(statusChange.captured) {
            assertThat(orderId).isEqualTo(order.id)
            assertThat(status).isEqualTo(OrderStatus.CREATED)
            assertThat(createdAt).isEqualTo(order.createdAt)
        }

        with(outboxEvent.captured) {
            assertThat(eventType).isEqualTo(ORDER_CREATED)
            assertThat(aggregateType).isEqualTo("Order")
            assertThat(aggregateId).isEqualTo(order.id.toString())
            // Keying by customer keeps all events of an order in one Kafka partition
            assertThat(aggregateKey).isEqualTo("customer-1")
            assertThat(targetTopic).isEqualTo(ORDER_EVENTS_TOPIC)
            assertThat(objectMapper.readValue(payload, OrderEvent::class.java)).isEqualTo(expectedEvent(order))
        }
    }

    private fun expectedEvent(order: Order) =
        OrderCreatedEvent(
            orderId = order.id.toString(),
            customerId = order.customerId,
            totalAmount = order.totalAmount,
            status = OrderStatus.CREATED,
            createdAt = order.createdAt,
            updatedAt = order.createdAt,
        )
}
