package org.example.talktripstatsservice.stream.service;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Kafka Streams 애플리케이션의 상태/메타데이터 조회 및 간단 제어를 담당합니다.
 */
@Service
@RequiredArgsConstructor
public class StreamsService {

    private static final Logger logger = LoggerFactory.getLogger(StreamsService.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public KafkaStreams getKafkaStreams() {
        try {
            return streamsBuilderFactoryBean.getKafkaStreams();
        } catch (Exception e) {
            logger.error("KafkaStreams 인스턴스 조회 실패", e);
            return null;
        }
    }

    public KafkaStreams.State getState() {
        KafkaStreams kafkaStreams = getKafkaStreams();
        return kafkaStreams != null ? kafkaStreams.state() : null;
    }

    public boolean isRunning() {
        return getState() == KafkaStreams.State.RUNNING;
    }

    public boolean isReady() {
        KafkaStreams kafkaStreams = getKafkaStreams();
        if (kafkaStreams == null) return false;
        KafkaStreams.State state = kafkaStreams.state();
        return state == KafkaStreams.State.RUNNING || state == KafkaStreams.State.REBALANCING;
    }

    public String getTopologyDescription() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return null;
            return "Topology 정보는 StreamsBuilderFactoryBean에서 직접 조회해야 합니다.";
        } catch (Exception e) {
            logger.error("Topology 정보 조회 실패", e);
            return null;
        }
    }

    @SuppressWarnings("deprecation")
    public List<StreamsMetadata> getAllStreamsMetadata() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return List.of();
            Collection<StreamsMetadata> metadataCollection = kafkaStreams.allMetadata();
            return new ArrayList<>(metadataCollection);
        } catch (Exception e) {
            logger.error("StreamsMetadata 조회 실패", e);
            return List.of();
        }
    }

    @SuppressWarnings("deprecation")
    public StreamsMetadata getStreamsMetadataForStore(String storeName, String key) {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return null;

            Collection<StreamsMetadata> allMetadata = kafkaStreams.allMetadata();
            return allMetadata.stream()
                    .filter(metadata -> metadata.stateStoreNames().contains(storeName))
                    .findFirst()
                    .orElse(null);
        } catch (Exception e) {
            logger.error("StreamsMetadata 조회 실패: storeName={}, key={}", storeName, key, e);
            return null;
        }
    }

    public boolean start() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return false;
            if (kafkaStreams.state() == KafkaStreams.State.RUNNING) return true;
            kafkaStreams.start();
            return true;
        } catch (Exception e) {
            logger.error("KafkaStreams 시작 실패", e);
            return false;
        }
    }

    public boolean stop() {
        try {
            KafkaStreams kafkaStreams = getKafkaStreams();
            if (kafkaStreams == null) return false;
            if (kafkaStreams.state() == KafkaStreams.State.NOT_RUNNING) return true;
            kafkaStreams.close();
            return true;
        } catch (Exception e) {
            logger.error("KafkaStreams 중지 실패", e);
            return false;
        }
    }

    public boolean restart() {
        try {
            boolean stopped = stop();
            if (!stopped) return false;
            Thread.sleep(1000);
            return start();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            logger.error("KafkaStreams 재시작 실패", e);
            return false;
        }
    }

    public String getStatusInfo() {
        KafkaStreams kafkaStreams = getKafkaStreams();
        if (kafkaStreams == null) return "KafkaStreams 인스턴스가 없습니다.";

        KafkaStreams.State state = kafkaStreams.state();
        String topology = getTopologyDescription();
        List<StreamsMetadata> metadata = getAllStreamsMetadata();

        StringBuilder info = new StringBuilder();
        info.append("KafkaStreams 상태 정보:\n");
        info.append("  - State: ").append(state).append("\n");
        info.append("  - Running: ").append(isRunning()).append("\n");
        info.append("  - Ready: ").append(isReady()).append("\n");
        if (topology != null) info.append("  - Topology: ").append(topology).append("\n");
        info.append("  - StreamsMetadata 개수: ").append(metadata.size()).append("\n");
        return info.toString();
    }
}

