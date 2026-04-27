package org.example.talktripstatsservice.stream.processor;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickEventDTO;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickStatResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class ProductClickProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ProductClickProcessor.class);

    @Value("${kafka.topics.product-click:product-click}")
    private String productClickTopic;

    @Value("${kafka.topics.product-click-stats:product-click-stats}")
    private String productClickStatsTopic;

    // Admin 통계 UI 요구: 00:00부터 30분 단위 tumbling window로 집계
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(30);
    private static final int TOP_N = 30;

    private final JsonSerde<ProductClickStatResponse> productClickStatSerde = createJsonSerde(ProductClickStatResponse.class);

    /**
     * List 타입은 제네릭 정보가 런타임에 소거되어 JsonSerde(List.class)로는
     * 복원 시 요소가 LinkedHashMap으로 역직렬화될 수 있습니다.
     * Streams state store/changelog 복원까지 안전하게 하려면 타입이 박힌 Serde가 필요합니다.
     */
    private final Serde<List<ProductClickStatResponse>> productClickStatListSerde =
            createTypedSerde(new TypeReference<>() {});

    private <T> JsonSerde<T> createJsonSerde(Class<T> clazz) {
        JsonSerde<T> serde = new JsonSerde<>(clazz);
        Map<String, Object> props = new HashMap<>();
        props.put("spring.json.trusted.packages", "*");
        props.put("spring.json.add.type.headers", false);
        props.put("spring.json.value.default.type", clazz.getName());
        serde.configure(props, false);
        return serde;
    }

    private <T> Serde<T> createTypedSerde(TypeReference<T> typeRef) {
        JsonSerializer<T> serializer = new JsonSerializer<>();
        JsonDeserializer<T> deserializer = new JsonDeserializer<>(typeRef);
        deserializer.addTrustedPackages("*");
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public void process(StreamsBuilder streamsBuilder) {
        logger.info("ProductClickProcessor Topology 구성 시작: inputTopic={}, outputTopic={}, windowSize={}, topN={}",
                productClickTopic, productClickStatsTopic, WINDOW_SIZE, TOP_N);

        JsonSerde<ProductClickEventDTO> productClickEventSerde = createJsonSerde(ProductClickEventDTO.class);

        KStream<String, ProductClickEventDTO> clickStream = streamsBuilder.stream(
                productClickTopic,
                Consumed.with(Serdes.String(), productClickEventSerde)
        );

        clickStream.peek((key, value) ->
                logger.info("상품 클릭 이벤트 처리: productId={}, memberId={}",
                        key, value != null ? value.memberId() : null)
        );

        productClickStatsStream(clickStream);

        logger.info("ProductClickProcessor Topology 구성 완료");
    }

    private void productClickStatsStream(KStream<String, ProductClickEventDTO> clickStream) {
        KStream<String, String> productIdStream = clickStream
                .filter((key, value) -> value != null && value.productId() != null)
                .selectKey((key, event) -> String.valueOf(event.productId()))
                .mapValues(event -> "1");

        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE);

        KTable<Windowed<String>, Long> clickCounts = productIdStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(tumblingWindow)
                .count(Materialized.as("product-click-count-store"));

        clickCounts
                .toStream()
                .map((windowedKey, count) -> {
                    String productId = windowedKey.key();
                    Instant windowStart = Instant.ofEpochMilli(windowedKey.window().start());
                    Instant windowEnd = Instant.ofEpochMilli(windowedKey.window().end());

                    ProductClickStatResponse stat = new ProductClickStatResponse(
                            productId,
                            count,
                            windowStart,
                            windowEnd
                    );

                    String windowKey = String.valueOf(windowedKey.window().start());
                    return KeyValue.pair(windowKey, stat);
                })
                .groupByKey(Grouped.with(Serdes.String(), productClickStatSerde))
                .aggregate(
                        ArrayList::new,
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate.stream()
                                    .sorted(Comparator.comparing(ProductClickStatResponse::clickCount).reversed())
                                    .limit(TOP_N)
                                    .collect(Collectors.toCollection(ArrayList::new));
                        },
                        Materialized.<String, List<ProductClickStatResponse>, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("product-click-top30-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(productClickStatListSerde)
                )
                .toStream()
                .to(productClickStatsTopic, Produced.with(Serdes.String(), productClickStatListSerde));
    }
}

