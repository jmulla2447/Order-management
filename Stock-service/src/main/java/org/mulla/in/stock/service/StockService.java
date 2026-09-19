package org.mulla.in.stock.service;

import org.mulla.in.base_service.dto.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


@Service
public class StockService {

    private static final Logger LOGGER = LoggerFactory.getLogger(StockService.class);

    @KafkaListener(topics = "${spring.order.topic.name}", groupId = "${spring.kafka.consumer.group_id}")
    public void consumeOrderEvent(OrderEvent event) {
        LOGGER.info(String.format("Stock serivce recived event %s", event.toString()));
        //Update Stock
    }
}
