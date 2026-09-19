package org.mulla.in.order.service;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mulla.in.base_service.dto.Order;
import org.mulla.in.base_service.dto.OrderEvent;
import org.mulla.in.base_service.dto.OrderStatus;
import org.mulla.in.order.service.process.OrderService;
import org.mulla.in.order.service.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import java.math.BigDecimal;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
@SpringBootTest
class OrderServiceTest {

    @MockitoBean
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @MockitoBean
    private OrderRepository orderRepository;

    @MockitoBean
    private NewTopic topic;

    @Autowired
    private OrderService orderService;

    @BeforeEach
    void setUp() {
        when(topic.name()).thenReturn("order-topic");
    }

    @Test
    void shouldRetryKafkaSend() {

        CompletableFuture<SendResult<String, OrderEvent>> failedFuture =
                new CompletableFuture<>();

        failedFuture.completeExceptionally(
                new RuntimeException("Kafka unavailable")
        );

        when(kafkaTemplate.send(any(Message.class)))
                .thenReturn(failedFuture);

        OrderEvent event = mock(OrderEvent.class);

        when(event.getEventId()).thenReturn(UUID.randomUUID());

        orderService.sendToKafka(event);

        verify(kafkaTemplate, times(2))
                .send(any(Message.class));
    }
}
