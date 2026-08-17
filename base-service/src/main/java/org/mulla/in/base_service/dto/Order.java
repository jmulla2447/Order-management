package org.mulla.in.base_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;
@Data
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class Order {
    private String orderId;
    private long orderQuantity;
    private BigDecimal orderPrice;
    private Date orderDate =  new Date();

}
