package com.example.dsf.order.application.queries

data class FindAllOrdersQuery(
    val page: Int = 0,
    val size: Int = 20
)