package org.example.talktripstatsservice.messaging.dto.product;

import java.time.Instant;

public record ProductClickStatResponse(
        String productId,
        long clickCount,
        Instant windowStart,
        Instant windowEnd
) {
}

