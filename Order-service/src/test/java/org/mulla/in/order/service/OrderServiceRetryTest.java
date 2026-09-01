package org.mulla.in.order.service;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.mulla.in.base_service.dto.Order;
import org.mulla.in.base_service.dto.OrderEvent;
import org.mulla.in.base_service.dto.OrderStatus;
import org.mulla.in.order.service.process.OrderService;
import org.mulla.in.order.service.repository.OrderRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(
        classes = OrderServiceRetryTest.TestConfig.class
)
class OrderServiceRetryTest {

    @SpringBootConfiguration
    @EnableKafkaRetryTopic
    @Import(OrderService.class)
    static class TestConfig {

        @Bean
        NewTopic topic() {
            return new NewTopic(
                    "test-topic",
                    1,
                    (short) 1
            );
        }
    }

    @MockitoBean
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @MockitoBean
    private OrderRepository orderRepository;

    @Autowired
    private OrderService orderService;

    @Test
    void retriesKafkaSendWithoutDuplicatingDbSave() {

        // Given
        OrderEvent event = sampleEvent();

        when(orderRepository.existsByEventId(anyString()))
                .thenReturn(false);

        CompletableFuture<SendResult<String, OrderEvent>> failedFuture =
                new CompletableFuture<>();

        failedFuture.completeExceptionally(
                new TimeoutException("Kafka timeout")
        );

        SendResult<String, OrderEvent> sendResult =
                mock(SendResult.class);

        CompletableFuture<SendResult<String, OrderEvent>> successfulFuture =
                CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(any(Message.class)))
                .thenReturn(failedFuture)
                .thenReturn(successfulFuture);

        // When
        orderService.orderCreate(event);

        // Then
        verify(orderRepository, times(1))
                .save(any());

        verify(kafkaTemplate, times(2))
                .send(any(Message.class));
    }

    private OrderEvent sampleEvent() {

        return new OrderEvent(
                UUID.randomUUID(),
                "test",
                OrderStatus.INITIALED,
                new Order(
                        UUID.randomUUID().toString(),
                        3L,
                        new BigDecimal("3.60"),
                        new Date()
                )
        );
    }
}
