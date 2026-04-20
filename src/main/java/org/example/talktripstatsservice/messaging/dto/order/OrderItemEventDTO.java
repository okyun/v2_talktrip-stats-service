package org.example.talktripstatsservice.messaging.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * 주문 아이템 이벤트 DTO
 * Kafka 이벤트에서 주문 아이템 정보를 전달하기 위한 DTO
 */
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderItemEventDTO {

    private Long productId;
    private Long productOptionId;
    private Integer quantity;
    private Integer price;
}

