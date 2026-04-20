package org.example.talktripstatsservice.stream.controller;

import lombok.RequiredArgsConstructor;
import org.example.talktripstatsservice.messaging.dto.order.OrderPurchaseStatResponse;
import org.example.talktripstatsservice.stream.service.OrderStreamsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 주문 관련 Kafka Streams 조회 API.
 *
 * - State Store에서 주문 구매 TOP30 조회
 * - Streams 준비 상태 확인
 */
@RestController
@RequestMapping("/api/streams/orders")
@RequiredArgsConstructor
public class OrderStreamController {

    private final OrderStreamsService orderStreamsService;

    @GetMapping("/purchases/top30")
    public ResponseEntity<List<OrderPurchaseStatResponse>> getTop30OrderPurchases(
            @RequestParam(required = false) Long windowStartTime
    ) {
        return ResponseEntity.ok(orderStreamsService.getTop30OrderPurchases(windowStartTime));
    }

    @GetMapping("/purchases/status")
    public ResponseEntity<Boolean> getOrderStreamsStatus() {
        return ResponseEntity.ok(orderStreamsService.isReady());
    }
}

