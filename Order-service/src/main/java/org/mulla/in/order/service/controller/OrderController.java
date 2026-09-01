package org.mulla.in.order.service.controller;


import org.mulla.in.base_service.dto.Order;
import org.mulla.in.order.service.process.OrderService;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.net.URI;

@Controller
@RequestMapping("/api/v1/management")
public class OrderController {

    final private OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    @PostMapping(value = "orders", consumes = MediaType.APPLICATION_JSON_VALUE,  produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity orderCreate(@RequestBody Order order) {
        service.orderCreate(OrderMapping.mapping(order));
        return ResponseEntity.created(URI.create("/orders/status")).build();
    }
}
