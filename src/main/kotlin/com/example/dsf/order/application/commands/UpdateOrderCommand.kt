package com.example.dsf.order.application.commands

import com.example.dsf.commons.OrderStatus
import java.util.UUID

class UpdateOrderCommand(
    val orderId: UUID,
    val customerId: String,
    val newStatus: OrderStatus,
)
