package org.example.talktripstatsservice.messaging.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.example.talktripstatsservice.messaging.dto.order.OrderCreatedEventDTO;
import org.example.talktripstatsservice.messaging.dto.order.OrderItemEventDTO;
import org.example.talktripstatsservice.messaging.dto.order.OrderPurchaseStatResponse;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickEventDTO;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickStatResponse;
import org.example.talktripstatsservice.statistics.StatsWindowUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Kafka 집계/이벤트를 Redis ZSET에 반영하는 Consumer.
 *
 * <ul>
 *   <li>product-click / order-created → ZINCRBY 윈도우 버킷 누적</li>
 *   <li>*-stats 토픽 → observe only (Streams TOP30 출력, Redis 미반영)</li>
 * </ul>
 */
@Component
@RequiredArgsConstructor
public class KafkaStatsRedisConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStatsRedisConsumer.class);

    @Value("${kafka.stats.redis.ttl-days:7}")
    private int statsTtlDays;

    private final StringRedisTemplate stringRedisTemplate;
    private final ObjectMapper objectMapper;

    private static final String PC_PREFIX = "trending:product-click:";
    private static final String OP_PREFIX = "trending:order-purchase:";
    private static final String PC_TS_PREFIX = "trending:product-click:ts:";
    private static final String OP_TS_PREFIX = "trending:order-purchase:ts:";

    private void incrementZSetCounts(String prefix, String windowStartKey, Map<String, Double> increments, Duration ttl) {
        if (prefix == null || windowStartKey == null || windowStartKey.isBlank()
                || increments == null || increments.isEmpty() || ttl == null) {
            return;
        }
        final String redisKey = Objects.requireNonNull(prefix + windowStartKey);
        ZSetOperations<String, String> zset = stringRedisTemplate.opsForZSet();
        for (Map.Entry<String, Double> entry : increments.entrySet()) {
            if (entry.getKey() == null || entry.getValue() == null || entry.getValue() <= 0) continue;
            zset.incrementScore(redisKey, entry.getKey(), entry.getValue());
        }
        stringRedisTemplate.expire(redisKey, ttl);
    }

    private void recordLatestEventTimes(
            String tsPrefix,
            String windowStartKey,
            Iterable<String> members,
            long eventMs,
            Duration ttl
    ) {
        if (tsPrefix == null || windowStartKey == null || windowStartKey.isBlank() || members == null || ttl == null) {
            return;
        }
        final String hashKey = Objects.requireNonNull(tsPrefix + windowStartKey);
        for (String member : members) {
            if (member == null || member.isBlank()) continue;
            String current = (String) stringRedisTemplate.opsForHash().get(hashKey, member);
            long nextMs = eventMs;
            if (current != null) {
                try {
                    nextMs = Math.max(eventMs, Long.parseLong(current));
                } catch (NumberFormatException ignored) {
                    nextMs = eventMs;
                }
            }
            stringRedisTemplate.opsForHash().put(hashKey, member, String.valueOf(nextMs));
        }
        stringRedisTemplate.expire(hashKey, ttl);
    }

    @KafkaListener(
            topics = "${kafka.topics.product-click:product-click}",
            groupId = "talktrip-stats-redis-product-click"
    )
    public void incrementProductClickFromEvent(
            @Payload String payloadJson,
            @Header(value = KafkaHeaders.RECEIVED_TIMESTAMP, required = false) Long kafkaTimestampMs
    ) {
        try {
            ProductClickEventDTO clickEvent = objectMapper.readValue(payloadJson, ProductClickEventDTO.class);
            if (clickEvent == null || clickEvent.productId() == null) {
                return;
            }

            long eventMs = resolveClickEventEpochMs(clickEvent, kafkaTimestampMs);
            String windowStartKey = String.valueOf(StatsWindowUtils.alignWindowStartMs(eventMs));
            Duration ttl = Duration.ofDays(statsTtlDays);

            Map<String, Double> increments = Map.of(String.valueOf(clickEvent.productId()), 1.0);
            incrementZSetCounts(PC_PREFIX, windowStartKey, increments, ttl);
            recordLatestEventTimes(PC_TS_PREFIX, windowStartKey, increments.keySet(), eventMs, ttl);

            logger.debug("[Stats][Redis] product-click ZINCRBY 반영: windowStartKey={}, productId={}",
                    windowStartKey, clickEvent.productId());
        } catch (Exception e) {
            logger.error("[Stats][Redis] product-click ZINCRBY 실패: error={}", e.getMessage(), e);
        }
    }

    @KafkaListener(
            topics = "${kafka.topics.product-click-stats:product-click-stats}",
            groupId = "talktrip-stats-redis-product-click-stats-log"
    )
    public void observeProductClickStats(
            @Payload String payloadJson,
            @Header(KafkaHeaders.RECEIVED_KEY) String windowStartKey
    ) {
        try {
            List<ProductClickStatResponse> topN = objectMapper.readValue(
                    payloadJson,
                    new TypeReference<>() {}
            );
            logger.debug("[Stats][Redis] product-click-stats 수신(Redis 미반영): windowStartKey={}, topN={}",
                    windowStartKey, topN != null ? topN.size() : 0);
        } catch (Exception e) {
            logger.warn("[Stats][Redis] product-click-stats 파싱 실패: windowStartKey={}, error={}",
                    windowStartKey, e.getMessage());
        }
    }

    @KafkaListener(
            topics = "${kafka.topics.order-created:order-created}",
            groupId = "talktrip-stats-redis-order-purchase"
    )
    public void incrementOrderPurchaseFromCreated(
            @Payload String payloadJson,
            @Header(value = KafkaHeaders.RECEIVED_TIMESTAMP, required = false) Long kafkaTimestampMs
    ) {
        try {
            OrderCreatedEventDTO orderEvent = objectMapper.readValue(payloadJson, OrderCreatedEventDTO.class);
            Map<String, Double> increments = extractPurchaseIncrements(orderEvent);
            if (increments.isEmpty()) {
                return;
            }

            long eventMs = resolveOrderEventEpochMs(orderEvent, kafkaTimestampMs);
            String windowStartKey = String.valueOf(StatsWindowUtils.alignWindowStartMs(eventMs));
            Duration ttl = Duration.ofDays(statsTtlDays);

            incrementZSetCounts(OP_PREFIX, windowStartKey, increments, ttl);
            recordLatestEventTimes(OP_TS_PREFIX, windowStartKey, increments.keySet(), eventMs, ttl);

            logger.debug("[Stats][Redis] order-created ZINCRBY 반영: windowStartKey={}, products={}",
                    windowStartKey, increments.size());
        } catch (Exception e) {
            logger.error("[Stats][Redis] order-created ZINCRBY 실패: error={}", e.getMessage(), e);
        }
    }

    @KafkaListener(
            topics = "${kafka.topics.order-purchase-stats:order-purchase-stats}",
            groupId = "talktrip-stats-redis-order-purchase-stats-log"
    )
    public void observeOrderPurchaseStats(
            @Payload String payloadJson,
            @Header(KafkaHeaders.RECEIVED_KEY) String windowStartKey
    ) {
        try {
            List<OrderPurchaseStatResponse> topN = objectMapper.readValue(
                    payloadJson,
                    new TypeReference<>() {}
            );
            logger.debug("[Stats][Redis] order-purchase-stats 수신(Redis 미반영): windowStartKey={}, topN={}",
                    windowStartKey, topN != null ? topN.size() : 0);
        } catch (Exception e) {
            logger.warn("[Stats][Redis] order-purchase-stats 파싱 실패: windowStartKey={}, error={}",
                    windowStartKey, e.getMessage());
        }
    }

    static Map<String, Double> extractPurchaseIncrements(OrderCreatedEventDTO orderEvent) {
        Map<String, Double> increments = new HashMap<>();
        if (orderEvent == null || orderEvent.getItems() == null) {
            return increments;
        }
        for (OrderItemEventDTO item : orderEvent.getItems()) {
            if (item == null || item.getProductId() == null) continue;
            int qty = item.getQuantity() != null && item.getQuantity() > 0 ? item.getQuantity() : 1;
            String member = String.valueOf(item.getProductId());
            increments.merge(member, (double) qty, Double::sum);
        }
        return increments;
    }

    static long resolveClickEventEpochMs(ProductClickEventDTO clickEvent, Long kafkaTimestampMs) {
        if (clickEvent != null && clickEvent.clickedAt() != null) {
            return clickEvent.clickedAt().toEpochMilli();
        }
        if (kafkaTimestampMs != null && kafkaTimestampMs > 0) {
            return kafkaTimestampMs;
        }
        return System.currentTimeMillis();
    }

    static long resolveOrderEventEpochMs(OrderCreatedEventDTO orderEvent, Long kafkaTimestampMs) {
        if (orderEvent != null && orderEvent.getCreatedAt() != null) {
            return orderEvent.getCreatedAt().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
        if (kafkaTimestampMs != null && kafkaTimestampMs > 0) {
            return kafkaTimestampMs;
        }
        return System.currentTimeMillis();
    }
}
