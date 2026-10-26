package com.example.dsf.order.application.commands

import java.math.BigDecimal
import java.util.UUID

class UpdateOrderTotalAmountCommand(
    val orderId: UUID,
    val customerId: String,
    val newTotalAmount: BigDecimal,
)
