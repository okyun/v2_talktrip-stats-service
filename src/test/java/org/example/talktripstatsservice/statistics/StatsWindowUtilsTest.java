package org.example.talktripstatsservice.statistics;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatsWindowUtilsTest {

    @Test
    void alignWindowStartMs_snapsToThirtyMinuteBoundary() {
        long windowStart = 1_710_000_000_000L;
        assertEquals(windowStart, StatsWindowUtils.alignWindowStartMs(windowStart));
        assertEquals(windowStart, StatsWindowUtils.alignWindowStartMs(windowStart + 1));
        assertEquals(windowStart, StatsWindowUtils.alignWindowStartMs(windowStart + StatsWindowUtils.WINDOW_SIZE_MS - 1));
        assertEquals(windowStart + StatsWindowUtils.WINDOW_SIZE_MS,
                StatsWindowUtils.alignWindowStartMs(windowStart + StatsWindowUtils.WINDOW_SIZE_MS));
    }
}
