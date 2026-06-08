package org.example.talktripstatsservice.statistics;

import java.time.Instant;

/**
 * Kafka Streams {@code OrderPurchaseProcessor} 와 동일한 30분 텀블링 윈도우 정렬.
 */
public final class StatsWindowUtils {

    public static final long WINDOW_SIZE_MS = 30L * 60L * 1000L;

    private StatsWindowUtils() {
    }

    public static long alignWindowStartMs(long epochMs) {
        return epochMs - (epochMs % WINDOW_SIZE_MS);
    }

    public static long alignWindowStartMs(Instant instant) {
        return alignWindowStartMs(instant.toEpochMilli());
    }
}
