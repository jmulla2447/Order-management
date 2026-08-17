package org.mulla.in.base_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    private String message;
    private OrderStatus status = OrderStatus.INITIALED;
    private Order order;
}
