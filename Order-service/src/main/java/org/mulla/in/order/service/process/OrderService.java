package org.mulla.in.order.service.process;

import org.apache.kafka.clients.admin.NewTopic;
import org.mulla.in.base_service.dto.OrderEvent;
import org.mulla.in.order.service.controller.OrderMapping;
import org.mulla.in.order.service.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderService.class);
    private NewTopic topic;

    private KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private OrderRepository orderRepository;

    public OrderService(NewTopic topic, KafkaTemplate<String, OrderEvent> kafkaTemplate, OrderRepository orderRepository) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
        this.orderRepository = orderRepository;
    }

    public void orderCreate(OrderEvent event) {
        LOGGER.info(String.format("Order need to sent stock : %s", event.toString()));
        Message<OrderEvent> message = MessageBuilder.withPayload(event).setHeader(KafkaHeaders.TOPIC, topic.name()).build();
        orderRepository.save(OrderMapping.entityMapping(event.getOrder()));
        kafkaTemplate.send(message);

    }
}
