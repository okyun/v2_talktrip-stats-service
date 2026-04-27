package org.example.talktripstatsservice.statistics.controller;

import lombok.RequiredArgsConstructor;
import org.example.talktripstatsservice.messaging.dto.order.OrderPurchaseStatResponse;
import org.example.talktripstatsservice.messaging.dto.product.ProductClickStatResponse;
import org.example.talktripstatsservice.stream.service.OrderStreamsService;
import org.example.talktripstatsservice.stream.service.ProductStreamsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

/**
 * 프론트(관리자 통계 페이지) 호환용 엔드포인트.
 *
 * 기존 프론트가 호출하는 경로(`/api/stats/...`)를 stats-service의 Streams 기반 구현으로 연결합니다.
 */
@RestController
@RequestMapping("/api/stats")
@RequiredArgsConstructor
public class StatsCompatController {

    private final ProductStreamsService productStreamsService;
    private final OrderStreamsService orderStreamsService;
    private static final int COMPAT_TOP_N = 3;
    private static final long WINDOW_SIZE_MS = 30L * 60L * 1000L; // ProductClickProcessor.WINDOW_SIZE 와 일치(30분)

    /**
     * 상품 클릭 통계(30분 윈도우) TOP N.
     *
     * - 기존 프론트: GET /api/stats/products/clicks?limit=10
     * - 내부 구현: Streams state store에서 TOP30 조회 후 limit로 자름
     */
    @GetMapping("/products/clicks")
    public ResponseEntity<List<ProductClickStatResponse>> getProductClickTopN(
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Long windowStartTime,
            @RequestParam(required = false, defaultValue = "false") boolean onlyCurrentWindow
    ) {
        int requested = limit != null ? limit : COMPAT_TOP_N;
        int effectiveLimit = Math.max(1, Math.min(requested, COMPAT_TOP_N));

        Long effectiveWindowStartTime = windowStartTime;
        if (onlyCurrentWindow) {
            // windowStartTime이 직접 전달되면 그 값을 최우선으로 사용(디버그/강제 조회)
            if (effectiveWindowStartTime == null) {
                effectiveWindowStartTime = currentTumblingWindowStartMs(Instant.now());
            }
        }

        var body = productStreamsService.getTopNProductClicks(effectiveLimit, effectiveWindowStartTime);

        // 프론트가 "현재 윈도우" UI를 맞출 수 있도록, 조회 키(윈도우) 메타를 헤더로 내려준다.
        if (onlyCurrentWindow && effectiveWindowStartTime != null) {
            long start = effectiveWindowStartTime;
            long end = start + WINDOW_SIZE_MS;
            return ResponseEntity.ok()
                    .header("X-TalkTrip-Window-Start-Ms", String.valueOf(start))
                    .header("X-TalkTrip-Window-End-Ms", String.valueOf(end))
                    .body(body);
        }

        return ResponseEntity.ok(body);
    }

    /**
     * 주문 구매 통계(30분 윈도우) TOP N.
     *
     * - 프론트: GET /api/stats/orders/purchases?limit=3&windowStartTime=...&onlyCurrentWindow=true
     * - 내부 구현: Streams state store에서 TOP30 조회 후 limit로 자름
     */
    @GetMapping("/orders/purchases")
    public ResponseEntity<List<OrderPurchaseStatResponse>> getOrderPurchaseTopN(
            @RequestParam(required = false) Integer limit,
            @RequestParam(required = false) Long windowStartTime,
            @RequestParam(required = false, defaultValue = "false") boolean onlyCurrentWindow
    ) {
        int requested = limit != null ? limit : COMPAT_TOP_N;
        int effectiveLimit = Math.max(1, Math.min(requested, COMPAT_TOP_N));

        Long effectiveWindowStartTime = windowStartTime;
        if (onlyCurrentWindow) {
            if (effectiveWindowStartTime == null) {
                effectiveWindowStartTime = currentTumblingWindowStartMs(Instant.now());
            }
        }

        List<OrderPurchaseStatResponse> body =
                orderStreamsService.getTopNOrderPurchases(effectiveLimit, effectiveWindowStartTime);

        if (onlyCurrentWindow && effectiveWindowStartTime != null) {
            long start = effectiveWindowStartTime;
            long end = start + WINDOW_SIZE_MS;
            return ResponseEntity.ok()
                    .header("X-TalkTrip-Window-Start-Ms", String.valueOf(start))
                    .header("X-TalkTrip-Window-End-Ms", String.valueOf(end))
                    .body(body);
        }

        return ResponseEntity.ok(body);
    }

    /**
     * Kafka Streams의 30분 텀블링 윈도우는 "epoch(1970) 기준"으로 30분 단위에 정렬됩니다.
     * (로컬 타임존이 아닌, 이벤트 타임/처리시간이 사용하는 동일한 기준과 맞춥니다)
     */
    private static long currentTumblingWindowStartMs(Instant now) {
        long t = now.toEpochMilli();
        return t - (t % WINDOW_SIZE_MS);
    }
}

