package com.example.dsf.commons

data class OrderCreatedEvent(
    val orderId: String,
    val customerId: String,
    val totalAmount: String,
    val status: String
)