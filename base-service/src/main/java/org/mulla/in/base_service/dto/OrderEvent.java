package org.mulla.in.base_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderEvent {
    private UUID eventId;
    private String message;
    private OrderStatus status = OrderStatus.INITIALED;
    private Order order;
}
