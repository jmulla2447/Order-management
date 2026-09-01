package org.mulla.in.order.service.process;

import org.apache.kafka.clients.admin.NewTopic;
import org.mulla.in.base_service.dto.OrderEvent;
import org.mulla.in.order.service.controller.OrderMapping;
import org.mulla.in.order.service.repository.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.resilience.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class OrderService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderService.class);

    private final NewTopic topic;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final OrderRepository orderRepository;

    public OrderService(NewTopic topic, KafkaTemplate<String, OrderEvent> kafkaTemplate, OrderRepository orderRepository) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
        this.orderRepository = orderRepository;
    }

    @Retryable(
            includes = KafkaSendFailedException.class,
            maxRetries = 2,
            delay = 1000,
            multiplier = 2.0
    )
    public void sendToKafka(OrderEvent event) {

        Message<OrderEvent> message = MessageBuilder
                .withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, topic.name())
                .build();

        try {
            kafkaTemplate.send(message).get(5, TimeUnit.SECONDS);
        }
        catch (TimeoutException | ExecutionException | InterruptedException e) {

            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            throw new KafkaSendFailedException(
                    "Kafka send failed for event " + event.getEventId(),
                    e
            );
        }
    }
    public void orderCreate(OrderEvent event) {

        String eventId = event.getEventId().toString();

        if (orderRepository.existsByEventId(eventId)) {
            LOGGER.info("Duplicate event {} — skipping", eventId);
            return;
        }

        orderRepository.save(OrderMapping.entityMapping(event));

        sendToKafka(event);
    }


}
