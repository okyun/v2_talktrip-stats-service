package org.example.talktripstatsservice.stream.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.List;
import java.util.Map;

/**
 * talktrip-stats-service용 Kafka Streams 기본 설정.
 *
 * back_end에 있던 Streams 구성(집계)을 이 서비스로 이관하기 위한 최소 설정입니다.
 */
@Configuration
public class KafkaStreamsConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaStreamsConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean(name = "defaultKafkaStreamsConfig")
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> props = kafkaProperties.getStreams().buildProperties(null);

        // Spring의 spring.kafka.bootstrap-servers를 StreamsConfig.bootstrap.servers로 보장 주입
        List<String> bootstrapServers = kafkaProperties.getBootstrapServers();
        if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
            props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", bootstrapServers));
        }

        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return new KafkaStreamsConfiguration(props);
    }
}

