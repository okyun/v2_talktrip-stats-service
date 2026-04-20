package org.example.talktripstatsservice.statistics.service;

import lombok.RequiredArgsConstructor;
import org.example.talktripstatsservice.messaging.dto.order.OrderPurchaseStatResponse;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickStatResponse;
import org.example.talktripstatsservice.stream.service.OrderStreamsService;
import org.example.talktripstatsservice.stream.service.ProductStreamsService;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 통계 조회 서비스 (State Store 기반).
 *
 * 기존 back_end의 StatisticsService 역할을 stats-service로 이관.
 */
@Service
@RequiredArgsConstructor
public class StatisticsService {

    private final ProductStreamsService productStreamsService;
    private final OrderStreamsService orderStreamsService;

    public List<ProductClickStatResponse> getTop30ProductClicks(Long windowStartTime) {
        return productStreamsService.getTop30ProductClicks(windowStartTime);
    }

    public List<OrderPurchaseStatResponse> getTop30OrderPurchases(Long windowStartTime) {
        return orderStreamsService.getTop30OrderPurchases(windowStartTime);
    }
}

