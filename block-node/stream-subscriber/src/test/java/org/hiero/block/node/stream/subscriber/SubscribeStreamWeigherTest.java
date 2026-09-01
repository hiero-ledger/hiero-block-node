// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.throttle.WeightClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SubscribeStreamWeigherTest {
    private static final long HISTORICAL_THRESHOLD_BLOCKS = 100;
    private static final long TIP_BLOCK_NUMBER = 1_000;

    private final SimpleInMemoryHistoricalBlockFacility blockProvider = new SimpleInMemoryHistoricalBlockFacility();
    private SubscribeStreamWeigher weigher;

    @BeforeEach
    void setUp() {
        final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
        availableBlocks.add(0, TIP_BLOCK_NUMBER);
        blockProvider.setTemporaryAvailableBlocks(availableBlocks);
        weigher = new SubscribeStreamWeigher(blockProvider, HISTORICAL_THRESHOLD_BLOCKS);
    }

    @Test
    @DisplayName("A pure live subscription (no fixed start block) is classified as STANDARD")
    void liveSubscriptionIsStandard() {
        assertEquals(WeightClass.STANDARD, classify(startBlockRequest(-1L)));
    }

    @Test
    @DisplayName("A start block within the historical threshold of the tip is classified as STANDARD")
    void withinThresholdIsStandard() {
        assertEquals(WeightClass.STANDARD, classify(startBlockRequest(TIP_BLOCK_NUMBER - HISTORICAL_THRESHOLD_BLOCKS)));
    }

    @Test
    @DisplayName("A start block further behind the tip than the historical threshold is classified as HEAVY")
    void beyondThresholdIsHeavy() {
        assertEquals(
                WeightClass.HEAVY, classify(startBlockRequest(TIP_BLOCK_NUMBER - HISTORICAL_THRESHOLD_BLOCKS - 1)));
    }

    @Test
    @DisplayName("A start block at or ahead of the tip (a future subscription) is classified as STANDARD")
    void futureStartBlockIsStandard() {
        assertEquals(WeightClass.STANDARD, classify(startBlockRequest(TIP_BLOCK_NUMBER)));
        assertEquals(WeightClass.STANDARD, classify(startBlockRequest(TIP_BLOCK_NUMBER + 1)));
    }

    @Test
    @DisplayName("Bytes with no start_block_number field default to block 0, same as a full parse would")
    void absentStartBlockNumberDefaultsToBlockZero() {
        // An empty message has no fields at all, so start_block_number defaults to 0 (proto3's
        // default for an absent scalar field) — exactly like a full `SubscribeStreamRequest.PROTOBUF.parse`
        // of the same bytes would. Block 0 is far behind TIP_BLOCK_NUMBER, so this is HEAVY.
        assertEquals(WeightClass.HEAVY, classify(Bytes.EMPTY));
    }

    private static Bytes startBlockRequest(final long startBlockNumber) {
        return SubscribeStreamRequest.PROTOBUF.toBytes(SubscribeStreamRequest.newBuilder()
                .startBlockNumber(startBlockNumber)
                .build());
    }

    private WeightClass classify(final Bytes requestBytes) {
        return weigher.classify(() -> "subscribeBlockStream", requestBytes);
    }
}
