package org.mulla.in.order.service.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Data
@Table(name = "orders", uniqueConstraints = @UniqueConstraint(columnNames = "event_id"))
public class OrderEntity {
    @Id
    private String orderId;
    @Column(nullable = false)
    private int quantity;
    private BigDecimal price;
    private Date orderDate;
    @Column(name = "event_id", nullable = false, unique = true)
    private String eventId;
}
