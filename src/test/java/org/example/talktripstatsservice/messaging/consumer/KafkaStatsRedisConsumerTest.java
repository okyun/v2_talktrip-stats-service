package org.example.talktripstatsservice.messaging.consumer;

import org.example.talktripstatsservice.messaging.dto.order.OrderCreatedEventDTO;
import org.example.talktripstatsservice.messaging.dto.order.OrderItemEventDTO;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaStatsRedisConsumerTest {

    @Test
    void extractPurchaseIncrements_sumsQuantityPerProduct() {
        OrderCreatedEventDTO event = OrderCreatedEventDTO.builder()
                .items(List.of(
                        OrderItemEventDTO.builder().productId(10L).quantity(2).build(),
                        OrderItemEventDTO.builder().productId(20L).quantity(1).build(),
                        OrderItemEventDTO.builder().productId(10L).quantity(3).build()
                ))
                .build();

        Map<String, Double> increments = KafkaStatsRedisConsumer.extractPurchaseIncrements(event);

        assertEquals(5.0, increments.get("10"));
        assertEquals(1.0, increments.get("20"));
    }

    @Test
    void resolveOrderEventEpochMs_prefersCreatedAt() {
        LocalDateTime createdAt = LocalDateTime.of(2026, 5, 27, 14, 15);
        OrderCreatedEventDTO event = OrderCreatedEventDTO.builder().createdAt(createdAt).build();
        long expected = createdAt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

        assertEquals(expected, KafkaStatsRedisConsumer.resolveOrderEventEpochMs(event, 1_710_000_000_000L));
    }

    @Test
    void resolveOrderEventEpochMs_fallsBackToKafkaTimestamp() {
        OrderCreatedEventDTO event = OrderCreatedEventDTO.builder().build();

        assertEquals(1_710_000_000_000L, KafkaStatsRedisConsumer.resolveOrderEventEpochMs(event, 1_710_000_000_000L));
    }
}
