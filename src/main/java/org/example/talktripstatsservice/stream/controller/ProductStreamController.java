package org.example.talktripstatsservice.stream.controller;

import lombok.RequiredArgsConstructor;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickStatResponse;
import org.example.talktripstatsservice.stream.service.ProductStreamsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 상품 관련 Kafka Streams 조회 API.
 *
 * - State Store에서 상품 클릭 TOP30 조회
 * - Streams 준비 상태 확인
 */
@RestController
@RequestMapping("/api/streams/products")
@RequiredArgsConstructor
public class ProductStreamController {

    private final ProductStreamsService productStreamsService;

    @GetMapping("/clicks/top30")
    public ResponseEntity<List<ProductClickStatResponse>> getTop30ProductClicks(
            @RequestParam(required = false) Long windowStartTime
    ) {
        return ResponseEntity.ok(productStreamsService.getTop30ProductClicks(windowStartTime));
    }

    @GetMapping("/clicks/status")
    public ResponseEntity<Boolean> getProductStreamsStatus() {
        return ResponseEntity.ok(productStreamsService.isReady());
    }
}

