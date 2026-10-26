package com.example.dsf.commons

import com.example.dsf.commons.OrderEventType.ORDER_CREATED
import com.example.dsf.commons.OrderEventType.ORDER_STATUS_UPDATED
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import java.math.BigDecimal

object OrderEventType {
    const val ORDER_CREATED = "ORDER_CREATED"
    const val ORDER_STATUS_UPDATED = "ORDER_STATUS_UPDATED"
}

data class OrderEventEnvelope(
    val type: String,
    val payload: OrderEvent
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.EXTERNAL_PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = OrderCreatedEvent::class, name = ORDER_CREATED),
    JsonSubTypes.Type(value = OrderStatusUpdatedEvent::class, name = ORDER_STATUS_UPDATED)
)
sealed interface OrderEvent

data class OrderCreatedEvent(
    val orderId: String,
    val customerId: String,
    val totalAmount: BigDecimal,
    val status: OrderStatus
) : OrderEvent

data class OrderStatusUpdatedEvent(
    val orderId: String,
    val customerId: String,
    val totalAmount: BigDecimal,
    val newStatus: OrderStatus
) : OrderEvent