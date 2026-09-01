package org.mulla.in.order.service.repository;

import org.mulla.in.order.service.entity.OrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface OrderRepository extends JpaRepository<OrderEntity, UUID> {
    boolean existsByEventId(String eventId);
}
