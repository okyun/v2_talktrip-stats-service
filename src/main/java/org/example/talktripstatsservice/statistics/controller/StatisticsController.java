package org.example.talktripstatsservice.statistics.controller;

import lombok.RequiredArgsConstructor;
import org.example.talktripstatsservice.messaging.dto.order.OrderPurchaseStatResponse;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickStatResponse;
import org.example.talktripstatsservice.statistics.service.StatisticsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 통계 조회 API.
 */
@RestController
@RequestMapping("/api/statistics")
@RequiredArgsConstructor
public class StatisticsController {

    private final StatisticsService statisticsService;

    @GetMapping("/product-clicks/top30")
    public ResponseEntity<List<ProductClickStatResponse>> getTop30ProductClicks(
            @RequestParam(required = false) Long windowStartTime
    ) {
        return ResponseEntity.ok(statisticsService.getTop30ProductClicks(windowStartTime));
    }

    @GetMapping("/order-purchases/top30")
    public ResponseEntity<List<OrderPurchaseStatResponse>> getTop30OrderPurchases(
            @RequestParam(required = false) Long windowStartTime
    ) {
        return ResponseEntity.ok(statisticsService.getTop30OrderPurchases(windowStartTime));
    }
}

