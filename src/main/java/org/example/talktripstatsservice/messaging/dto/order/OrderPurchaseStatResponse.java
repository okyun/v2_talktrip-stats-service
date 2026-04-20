package org.example.talktripstatsservice.messaging.dto.order;

import java.time.Instant;

/**
 * 주문 구매 통계 응답 DTO
 * 15분 간격 윈도우별 주문 구매 통계 정보
 */
public record OrderPurchaseStatResponse(
        Long productId,
        long purchaseCount,
        Instant windowStart,
        Instant windowEnd
) {
}

