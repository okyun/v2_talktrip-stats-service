package org.example.talktripstatsservice.messaging.consumer;

import lombok.RequiredArgsConstructor;
import org.example.talktripstatsservice.messaging.dto.order.OrderPurchaseStatResponse;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickStatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Kafka Streams 집계 결과를 Redis에 반영하는 Consumer
 *
 * - Streams: product-click → product-click-stats / order-created → order-purchase-stats
 * - Listener: product-click-stats, order-purchase-stats → Redis(ZSET)에 TOP30 저장
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

    private void writeTopNToZSet(String key, List<? extends Object> items, Duration ttl) {
        if (key == null || ttl == null) return;
        final String redisKey = Objects.requireNonNull(key);
        // ZSET을 "스냅샷"으로 쓰기 위해 기존 키를 지우고 TopN만 다시 세팅한다.
        stringRedisTemplate.delete(redisKey);
        ZSetOperations<String, String> zset = stringRedisTemplate.opsForZSet();
        for (Object item : items) {
            if (item instanceof ProductClickStatResponse pc) {
                if (pc.productId() == null) continue;
                String member = Objects.requireNonNull(String.valueOf(pc.productId()));
                zset.add(redisKey, member, Double.valueOf(pc.clickCount()));
            } else if (item instanceof OrderPurchaseStatResponse op) {
                if (op.productId() == null) continue;
                String member = Objects.requireNonNull(String.valueOf(op.productId()));
                zset.add(redisKey, member, Double.valueOf(op.purchaseCount()));
            }
        }
        stringRedisTemplate.expire(redisKey, ttl);
    }

    @KafkaListener(
            topics = "${kafka.topics.product-click-stats:product-click-stats}",
            groupId = "talktrip-stats-redis-product-click"
    )
    public void updateProductClickStats(
            @Payload String payloadJson,
            @Header(KafkaHeaders.RECEIVED_KEY) String windowStartKey
    ) {
        try {
            Duration ttl = Duration.ofDays(statsTtlDays);

            List<ProductClickStatResponse> topN = objectMapper.readValue(
                    payloadJson,
                    new TypeReference<>() {}
            );

            writeTopNToZSet(PC_PREFIX + "latest", topN, ttl);
            if (windowStartKey != null) writeTopNToZSet(PC_PREFIX + windowStartKey, topN, ttl);

            logger.info("[Stats][Redis] product-click-stats 반영 완료: windowStartKey={}, length={}",
                    windowStartKey, payloadJson != null ? payloadJson.length() : 0);
        } catch (Exception e) {
            logger.error("[Stats][Redis] product-click-stats Redis 반영 실패: windowStartKey={}, error={}",
                    windowStartKey, e.getMessage(), e);
        }
    }

    @KafkaListener(
            topics = "${kafka.topics.order-purchase-stats:order-purchase-stats}",
            groupId = "talktrip-stats-redis-order-purchase"
    )
    public void updateOrderPurchaseStats(
            @Payload String payloadJson,
            @Header(KafkaHeaders.RECEIVED_KEY) String windowStartKey
    ) {
        try {
            Duration ttl = Duration.ofDays(statsTtlDays);

            List<OrderPurchaseStatResponse> topN = objectMapper.readValue(
                    payloadJson,
                    new TypeReference<>() {}
            );

            writeTopNToZSet(OP_PREFIX + "latest", topN, ttl);
            if (windowStartKey != null) writeTopNToZSet(OP_PREFIX + windowStartKey, topN, ttl);

            logger.info("[Stats][Redis] order-purchase-stats 반영 완료: windowStartKey={}, length={}",
                    windowStartKey, payloadJson != null ? payloadJson.length() : 0);
        } catch (Exception e) {
            logger.error("[Stats][Redis] order-purchase-stats Redis 반영 실패: windowStartKey={}, error={}",
                    windowStartKey, e.getMessage(), e);
        }
    }
}

