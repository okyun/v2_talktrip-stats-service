package org.example.talktripstatsservice.messaging.dto.order;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 주문 생성 이벤트 DTO
 * Kafka로 발행되는 주문 생성 이벤트의 데이터 구조
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCreatedEventDTO {

    @JsonProperty("orderId")
    private Long orderId;

    @JsonProperty("orderCode")
    private String orderCode;

    @JsonProperty("memberId")
    private Long memberId;

    @JsonProperty("totalPrice")
    private Integer totalPrice;

    @JsonProperty("orderStatus")
    private String orderStatus;

    @JsonProperty("createdAt")
    private LocalDateTime createdAt; // 주문 생성 시각

    @JsonProperty("items")
    private List<OrderItemEventDTO> items;

    public static OrderCreatedEventDTO of(Long orderId, Long memberId, List<OrderItemEventDTO> items) {
        return OrderCreatedEventDTO.builder()
                .orderId(orderId)
                .memberId(memberId)
                .items(items)
                .createdAt(LocalDateTime.now())
                .build();
    }
}
