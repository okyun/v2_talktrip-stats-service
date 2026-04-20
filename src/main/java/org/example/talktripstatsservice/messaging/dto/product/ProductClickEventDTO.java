package org.example.talktripstatsservice.messaging.dto.product;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * 상품 클릭 이벤트 DTO
 *
 * Kafka로 발행되는 상품 클릭 이벤트를 표현하는 DTO입니다.
 * JSON 형식으로 직렬화/역직렬화됩니다.
 */
public record ProductClickEventDTO(
        @JsonProperty("productId")
        Long productId,

        @JsonProperty("memberId")
        Long memberId, // null 가능 (비회원 클릭 시)

        @JsonProperty("clickedAt")
        Instant clickedAt // 클릭 시각 (UTC)
) {
    public static ProductClickEventDTO of(Long productId) {
        return new ProductClickEventDTO(productId, null, Instant.now());
    }

    public static ProductClickEventDTO of(Long productId, Long memberId) {
        return new ProductClickEventDTO(productId, memberId, Instant.now());
    }
}

