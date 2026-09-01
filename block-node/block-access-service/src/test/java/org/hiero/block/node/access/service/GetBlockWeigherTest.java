// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.node.app.fixtures.plugintest.SimpleBlockRangeSet;
import org.hiero.block.node.app.fixtures.plugintest.SimpleInMemoryHistoricalBlockFacility;
import org.hiero.block.node.spi.throttle.WeightClass;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class GetBlockWeigherTest {
    private static final long HISTORICAL_THRESHOLD_BLOCKS = 100;
    private static final long TIP_BLOCK_NUMBER = 1_000;

    private final SimpleInMemoryHistoricalBlockFacility blockProvider = new SimpleInMemoryHistoricalBlockFacility();
    private GetBlockWeigher weigher;

    @BeforeEach
    void setUp() {
        final SimpleBlockRangeSet availableBlocks = new SimpleBlockRangeSet();
        availableBlocks.add(0, TIP_BLOCK_NUMBER);
        blockProvider.setTemporaryAvailableBlocks(availableBlocks);
        weigher = new GetBlockWeigher(blockProvider, HISTORICAL_THRESHOLD_BLOCKS);
    }

    @Test
    @DisplayName("A request within the historical threshold of the tip is classified as STANDARD")
    void withinThresholdIsStandard() {
        assertEquals(
                WeightClass.STANDARD, classify(blockNumberRequest(TIP_BLOCK_NUMBER - HISTORICAL_THRESHOLD_BLOCKS)));
    }

    @Test
    @DisplayName("A request further behind the tip than the historical threshold is classified as HEAVY")
    void beyondThresholdIsHeavy() {
        assertEquals(
                WeightClass.HEAVY, classify(blockNumberRequest(TIP_BLOCK_NUMBER - HISTORICAL_THRESHOLD_BLOCKS - 1)));
    }

    @Test
    @DisplayName("A request for the tip block itself is classified as STANDARD")
    void tipBlockIsStandard() {
        assertEquals(WeightClass.STANDARD, classify(blockNumberRequest(TIP_BLOCK_NUMBER)));
    }

    @Test
    @DisplayName("retrieveLatest requests, and malformed bytes, are always classified as STANDARD")
    void latestAndMalformedRequestsAreStandard() {
        final BlockRequest latest =
                BlockRequest.newBuilder().retrieveLatest(true).build();
        assertEquals(WeightClass.STANDARD, classify(BlockRequest.PROTOBUF.toBytes(latest)));
        assertEquals(WeightClass.STANDARD, classify(Bytes.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF})));
    }

    private static Bytes blockNumberRequest(final long blockNumber) {
        return BlockRequest.PROTOBUF.toBytes(
                BlockRequest.newBuilder().blockNumber(blockNumber).build());
    }

    private WeightClass classify(final Bytes requestBytes) {
        return weigher.classify(() -> "getBlock", requestBytes);
    }
}
