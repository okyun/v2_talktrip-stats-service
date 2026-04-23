package org.example.talktripstatsservice.stream.topology;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.example.talktripstatsservice.stream.processor.OrderPurchaseProcessor;
import org.example.talktripstatsservice.stream.processor.ProductClickProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Kafka Streams Topology 설정
 *
 * 상품 클릭 통계와 주문 구매 통계를 처리하는 Topology를 정의합니다.
  * 각 Processor의 윈도우 설정(WINDOW_SIZE)을 기준으로 집계하여 TOP 30을 추출합니다.
 */
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class StatisticsTopology {

    private static final Logger logger = LoggerFactory.getLogger(StatisticsTopology.class);

    private final StreamsBuilder streamsBuilder;
    private final ProductClickProcessor productClickProcessor;
    private final OrderPurchaseProcessor orderPurchaseProcessor;

    @PostConstruct
    public void buildTopology() {
        logger.info("Kafka Streams Topology 구성 시작");

        productClickProcessor.process(streamsBuilder);
        orderPurchaseProcessor.process(streamsBuilder);

        logger.info("Kafka Streams Topology 구성 완료");
    }
}

