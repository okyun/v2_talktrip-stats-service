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
import org.example.talktripstatsservice.messaging.dto.order.OrderCreatedEventDTO;
import org.example.talktripstatsservice.messaging.dto.order.OrderItemEventDTO;
import org.example.talktripstatsservice.messaging.dto.order.OrderPurchaseStatResponse;
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
public class OrderPurchaseProcessor {

    private static final Logger logger = LoggerFactory.getLogger(OrderPurchaseProcessor.class);

    @Value("${kafka.topics.order-created:order-created}")
    private String orderCreatedTopic;

    @Value("${kafka.topics.order-purchase-stats:order-purchase-stats}")
    private String orderPurchaseStatsTopic;

    // Admin 통계 UI 요구: 00:00부터 30분 단위 tumbling window로 집계
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(30);
    private static final int TOP_N = 30;

    private final JsonSerde<OrderPurchaseStatResponse> orderPurchaseStatSerde = createJsonSerde(OrderPurchaseStatResponse.class);

    /**
     * List 타입은 제네릭 정보 소거로 인해 복원 시 LinkedHashMap으로 풀릴 수 있어,
     * Streams state store/changelog 복원까지 안전한 타입 고정 Serde를 사용합니다.
     */
    private final Serde<List<OrderPurchaseStatResponse>> orderPurchaseStatListSerde =
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
        logger.info("OrderPurchaseProcessor Topology 구성 시작: inputTopic={}, outputTopic={}, windowSize={}, topN={}",
                orderCreatedTopic, orderPurchaseStatsTopic, WINDOW_SIZE, TOP_N);

        JsonSerde<OrderCreatedEventDTO> orderCreatedEventSerde = createJsonSerde(OrderCreatedEventDTO.class);

        KStream<String, OrderCreatedEventDTO> orderStream = streamsBuilder.stream(
                orderCreatedTopic,
                Consumed.with(Serdes.String(), orderCreatedEventSerde)
        );

        orderPurchaseStatsStream(orderStream);

        logger.info("OrderPurchaseProcessor Topology 구성 완료");
    }

    /**
     * 주문 생성 스트림을 상품 단위로 펼친 뒤 30분 텀블링 윈도우로 집계하고, 윈도우별 구매 수 TOP {@value #TOP_N}을 출력 토픽으로 보냅니다.
     *
     * <p>윈도우 단계:
     * <ol>
     *   <li>{@code flatMap} — 주문 항목을 productId 키로 펼치고, 수량만큼 레코드를 복제해 구매 건수로 셉니다.</li>
     *   <li>{@code groupByKey} — 집계 키를 {@code productId}로 고정합니다.</li>
     *   <li>{@code windowedBy(TimeWindows)} — 이벤트 타임스탬프(기본: 레코드 timestamp)를 기준으로
     *       겹치지 않는 고정 길이 구간(텀블링)에 레코드를 배치합니다. 길이는 {@link #WINDOW_SIZE}(30분)이며,
     *       Admin 통계 UI의 30분 슬롯 요구에 맞춥니다.</li>
     *   <li>{@code count} — (윈도우, productId)별 구매 건수를 누적합니다. 상태는
     *       {@code order-purchase-count-store}에 저장됩니다.</li>
     * </ol>
     *
     * <p>{@link TimeWindows#ofSizeWithNoGrace(Duration)} 은 grace period가 없는 텀블링 윈도우입니다.
     * 윈도우가 닫힌 뒤 늦게 도착한 레코드는 이 집계에 포함되지 않습니다(지연 허용 없음).
     * 경계 시각은 Kafka Streams 기본 정렬(UTC epoch 기준 고정 간격)을 따릅니다.
     *
     * <p>이후 {@code toStream}에서 {@link Windowed} 키의 {@code window().start()}/{@code end()}를
     * {@link OrderPurchaseStatResponse}에 실어, 동일 윈도우 시작 시각({@code windowStartMs}) 단위로 다시 묶어
     * TOP N만 유지한 뒤 {@code order-purchase-stats}로 publish 합니다.
     */
    private void orderPurchaseStatsStream(KStream<String, OrderCreatedEventDTO> orderStream) {
        KStream<String, String> productIdStream = orderStream
                .flatMap((key, orderEvent) -> {
                    List<KeyValue<String, String>> result = new ArrayList<>();

                    try {
                        if (orderEvent != null && orderEvent.getItems() != null) {
                            for (OrderItemEventDTO item : orderEvent.getItems()) {
                                if (item.getProductId() != null) {
                                    String productId = String.valueOf(item.getProductId());
                                    int qty = item.getQuantity() != null ? item.getQuantity() : 1;
                                    for (int i = 0; i < qty; i++) {
                                        result.add(KeyValue.pair(productId, productId));
                                    }
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error("주문 이벤트에서 productId 추출 실패: key={}, value={}", key, orderEvent, e);
                    }

                    return result;
                });

        // 30분 텀블링: 구간마다 productId별 구매 건수를 독립 집계 (grace 없음 → 지연 레코드 제외)
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE);

        KTable<Windowed<String>, Long> purchaseCounts = productIdStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(tumblingWindow)
                .count(Materialized.as("order-purchase-count-store"));

        purchaseCounts
                .toStream()
                .map((windowedKey, count) -> {
                    String productId = windowedKey.key();
                    Instant windowStart = Instant.ofEpochMilli(windowedKey.window().start());
                    Instant windowEnd = Instant.ofEpochMilli(windowedKey.window().end());

                    OrderPurchaseStatResponse stat = new OrderPurchaseStatResponse(
                            Long.parseLong(productId),
                            count,
                            windowStart,
                            windowEnd
                    );

                    String windowKey = String.valueOf(windowedKey.window().start());
                    return KeyValue.pair(windowKey, stat);
                })
                .groupByKey(Grouped.with(Serdes.String(), orderPurchaseStatSerde))
                .aggregate(
                        ArrayList::new,
                        (key, value, aggregate) -> {
                            aggregate.add(value);
                            return aggregate.stream()
                                    .sorted(Comparator.comparing(OrderPurchaseStatResponse::purchaseCount).reversed())
                                    .limit(TOP_N)
                                    .collect(Collectors.toCollection(ArrayList::new));
                        },
                        Materialized.<String, List<OrderPurchaseStatResponse>, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as("order-purchase-top30-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(orderPurchaseStatListSerde)
                )
                .toStream()
                .to(orderPurchaseStatsTopic, Produced.with(Serdes.String(), orderPurchaseStatListSerde));
    }
}

