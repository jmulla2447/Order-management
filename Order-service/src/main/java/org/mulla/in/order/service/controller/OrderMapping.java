package org.mulla.in.order.service.controller;

import org.mulla.in.base_service.dto.Order;
import org.mulla.in.base_service.dto.OrderEvent;
import org.mulla.in.base_service.dto.OrderStatus;
import org.mulla.in.order.service.entity.OrderEntity;

import java.util.Date;
import java.util.UUID;

public class OrderMapping {

    public static OrderEvent mapping(Order order) {
        order.setOrderId(UUID.randomUUID().toString());
        order.setOrderDate(new Date());
        return OrderEvent.builder().order(order).status(OrderStatus.INITIALED)
                .message("Order is created and Stock need to reduce").build();
    }

    public static OrderEntity entityMapping(Order order){
        return new OrderEntity(order.getOrderId(), (int)order.getOrderQuantity(), order.getOrderPrice(), order.getOrderDate());
    }
}
