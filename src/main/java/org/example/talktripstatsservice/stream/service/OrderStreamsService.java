package org.example.talktripstatsservice.stream.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.example.talktripstatsservice.messaging.dto.order.OrderPurchaseStatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 주문 관련 Kafka Streams 서비스
 *
 * - State Store에서 주문 구매 TOP30 조회
 * - StreamsMetadata 조회
 */
@Service
@RequiredArgsConstructor
public class OrderStreamsService {

    private static final Logger logger = LoggerFactory.getLogger(OrderStreamsService.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private static final String ORDER_PURCHASE_TOP30_STORE = "order-purchase-top30-store";
    private static final String ORDER_PURCHASE_COUNT_STORE = "order-purchase-count-store";

    private KafkaStreams getKafkaStreams() {
        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                logger.warn("주문 Streams의 KafkaStreams가 아직 준비되지 않았습니다. state={}",
                        kafkaStreams != null ? kafkaStreams.state() : "null");
                return null;
            }
            return kafkaStreams;
        } catch (Exception e) {
            logger.error("KafkaStreams 인스턴스 조회 실패", e);
            return null;
        }
    }

    public List<OrderPurchaseStatResponse> getTop30OrderPurchases(Long windowStartTime) {
        return getTopNOrderPurchases(30, windowStartTime);
    }

    public List<OrderPurchaseStatResponse> getTopNOrderPurchases(int limit, Long windowStartTime) {
        try {
            int effectiveLimit = Math.max(1, limit);

            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return List.of();

            ReadOnlyKeyValueStore<String, List<OrderPurchaseStatResponse>> store =
                    kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                            ORDER_PURCHASE_TOP30_STORE,
                            QueryableStoreTypes.keyValueStore()
                    ));

            String key = windowStartTime != null ? String.valueOf(windowStartTime) : null;
            if (key != null) {
                List<OrderPurchaseStatResponse> result = store.get(key);
                if (result == null) return List.of();
                return result.stream()
                        .sorted(Comparator.comparing(OrderPurchaseStatResponse::purchaseCount).reversed())
                        .limit(effectiveLimit)
                        .collect(Collectors.toList());
            }

            List<OrderPurchaseStatResponse> allResults = new ArrayList<>();
            try (KeyValueIterator<String, List<OrderPurchaseStatResponse>> iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, List<OrderPurchaseStatResponse>> entry = iterator.next();
                    if (entry.value != null) allResults.addAll(entry.value);
                }
            }

            return allResults.stream()
                    .sorted(Comparator.comparing(OrderPurchaseStatResponse::purchaseCount).reversed())
                    .limit(effectiveLimit)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("주문 구매 통계 조회 실패", e);
            return List.of();
        }
    }

    @SuppressWarnings("deprecation")
    public List<StreamsMetadata> getOrderStreamsMetadata() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return List.of();

            Collection<StreamsMetadata> allMetadata = kafkaStreams.allMetadata();
            return allMetadata.stream()
                    .filter(metadata -> metadata.stateStoreNames().contains(ORDER_PURCHASE_TOP30_STORE)
                            || metadata.stateStoreNames().contains(ORDER_PURCHASE_COUNT_STORE))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("주문 StreamsMetadata 조회 실패", e);
            return List.of();
        }
    }

    public ReadOnlyKeyValueStore<String, List<OrderPurchaseStatResponse>> getOrderPurchaseTop30Store() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return null;

            return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                    ORDER_PURCHASE_TOP30_STORE,
                    QueryableStoreTypes.keyValueStore()
            ));
        } catch (Exception e) {
            logger.error("주문 구매 통계 Store 조회 실패", e);
            return null;
        }
    }

    public boolean isReady() {
        return getKafkaStreams() != null;
    }
}

