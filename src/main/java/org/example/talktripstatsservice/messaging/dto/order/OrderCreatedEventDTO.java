package org.example.talktripstatsservice.messaging.dto.order;

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

    private Long orderId;
    private String orderCode;
    private Long memberId;
    private Integer totalPrice;
    private String orderStatus;
    private LocalDateTime createdAt;
    private List<OrderItemEventDTO> items;
}

