package com.example.dsf.order.application

import com.example.dsf.order.application.queries.FindAllOrdersQuery
import com.example.dsf.order.domain.Order
import com.example.dsf.order.domain.OrderRepository
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class GetOrdersService(
    private val orderRepository: OrderRepository
) {

    @Transactional(readOnly = true)
    fun execute(query: FindAllOrdersQuery): List<Order> = orderRepository.findAll()
}