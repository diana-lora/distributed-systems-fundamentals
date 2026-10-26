package com.example.dsf.order.infra.web

import com.example.dsf.order.application.CreateOrderService
import com.example.dsf.order.application.GetOrdersService
import com.example.dsf.order.application.UpdateOrderService
import com.example.dsf.order.application.commands.PlaceOrderCommand
import com.example.dsf.order.application.commands.UpdateOrderCommand
import com.example.dsf.order.application.queries.FindAllOrdersQuery
import com.example.dsf.order.infra.web.dtos.CreateOrderRequest
import com.example.dsf.order.infra.web.dtos.OrderResponse
import com.example.dsf.order.infra.web.dtos.UpdateOrderStatusRequest
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/orders")
class OrderController(
    private val createOrderService: CreateOrderService,
    private val updateOrderService: UpdateOrderService,
    private val getOrdersService: GetOrdersService
) {
    @PostMapping
    fun createOrder(@RequestBody request: CreateOrderRequest): ResponseEntity<OrderResponse> {
        val command = PlaceOrderCommand(
            customerId = request.customerId,
            totalAmount = request.totalAmount
        )

        val savedOrder = createOrderService.execute(command)

        return ResponseEntity
            .status(HttpStatus.CREATED)
            .body(OrderResponse.fromDomain(savedOrder))
    }

    @PutMapping
    fun updateOrder(@RequestBody request: UpdateOrderStatusRequest) {
        val command = UpdateOrderCommand(
            orderId = request.orderId,
            customerId = request.customerId,
            newStatus = request.status
        )

        updateOrderService.execute(command)
    }

    @GetMapping
    fun getAllOrders(
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "20") size: Int
    ): ResponseEntity<List<OrderResponse>> {
        val query = FindAllOrdersQuery(page = page, size = size)

        val ordersList = getOrdersService.execute(query)

        val responseList = ordersList.map { OrderResponse.fromDomain(it) }
        return ResponseEntity.ok(responseList)
    }
}