package org.example.talktripstatsservice.stream.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickStatResponse;
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
 * 상품 관련 Kafka Streams 서비스
 *
 * - State Store에서 상품 클릭 TOP30 조회
 * - StreamsMetadata 조회
 */
@Service
@RequiredArgsConstructor
public class ProductStreamsService {

    private static final Logger logger = LoggerFactory.getLogger(ProductStreamsService.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private static final String PRODUCT_CLICK_TOP30_STORE = "product-click-top30-store";
    private static final String PRODUCT_CLICK_COUNT_STORE = "product-click-count-store";

    private KafkaStreams getKafkaStreams() {
        try {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
                logger.warn("상품 Streams의 KafkaStreams가 아직 준비되지 않았습니다. state={}",
                        kafkaStreams != null ? kafkaStreams.state() : "null");
                return null;
            }
            return kafkaStreams;
        } catch (Exception e) {
            logger.error("KafkaStreams 인스턴스 조회 실패", e);
            return null;
        }
    }

    public List<ProductClickStatResponse> getTop30ProductClicks(Long windowStartTime) {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return List.of();

            ReadOnlyKeyValueStore<String, List<ProductClickStatResponse>> store =
                    kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                            PRODUCT_CLICK_TOP30_STORE,
                            QueryableStoreTypes.keyValueStore()
                    ));

            String key = windowStartTime != null ? String.valueOf(windowStartTime) : null;
            if (key != null) {
                List<ProductClickStatResponse> result = store.get(key);
                return result != null ? result : List.of();
            }

            List<ProductClickStatResponse> allResults = new ArrayList<>();
            try (KeyValueIterator<String, List<ProductClickStatResponse>> iterator = store.all()) {
                while (iterator.hasNext()) {
                    KeyValue<String, List<ProductClickStatResponse>> entry = iterator.next();
                    if (entry.value != null) allResults.addAll(entry.value);
                }
            }

            return allResults.stream()
                    .sorted(Comparator.comparing(ProductClickStatResponse::clickCount).reversed())
                    .limit(30)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("상품 클릭 통계 조회 실패", e);
            return List.of();
        }
    }

    @SuppressWarnings("deprecation")
    public List<StreamsMetadata> getProductStreamsMetadata() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return List.of();

            Collection<StreamsMetadata> allMetadata = kafkaStreams.allMetadata();
            return allMetadata.stream()
                    .filter(metadata -> metadata.stateStoreNames().contains(PRODUCT_CLICK_TOP30_STORE)
                            || metadata.stateStoreNames().contains(PRODUCT_CLICK_COUNT_STORE))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            logger.error("상품 StreamsMetadata 조회 실패", e);
            return List.of();
        }
    }

    public ReadOnlyKeyValueStore<String, List<ProductClickStatResponse>> getProductClickTop30Store() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return null;

            return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                    PRODUCT_CLICK_TOP30_STORE,
                    QueryableStoreTypes.keyValueStore()
            ));
        } catch (Exception e) {
            logger.error("상품 클릭 통계 Store 조회 실패", e);
            return null;
        }
    }

    public boolean isReady() {
        return getKafkaStreams() != null;
    }
}

