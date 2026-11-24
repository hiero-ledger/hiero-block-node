// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.access.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.hedera.pbj.runtime.grpc.ServiceInterface;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.concurrent.atomic.AtomicInteger;
import org.hiero.block.api.BlockRequest;
import org.hiero.block.node.spi.throttle.WeightClass;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/// Verifies [GetBlockWeigher] classifies `getBlock` requests by distance from the tip, reading the
/// block number straight off the wire.
class GetBlockWeigherTest {
    private static final ServiceInterface.Method METHOD = () -> "getBlock";
    private static final long TIP = 100_000L;
    private static final long THRESHOLD = 1_000L;

    private final GetBlockWeigher weigher = new GetBlockWeigher(() -> TIP, THRESHOLD);

    private WeightClass classify(final BlockRequest request) {
        return weigher.classify(METHOD, BlockRequest.PROTOBUF.toBytes(request));
    }

    private static BlockRequest number(final long blockNumber) {
        return BlockRequest.newBuilder().blockNumber(blockNumber).build();
    }

    @Test
    @DisplayName("A block near the tip is STANDARD")
    void recentBlockIsStandard() {
        assertEquals(WeightClass.STANDARD, classify(number(TIP - 10)));
        assertEquals(WeightClass.STANDARD, classify(number(TIP)));
    }

    @Test
    @DisplayName("A block beyond the threshold behind the tip is HEAVY")
    void oldBlockIsHeavy() {
        assertEquals(WeightClass.HEAVY, classify(number(0)));
        assertEquals(WeightClass.HEAVY, classify(number(TIP - THRESHOLD - 1)));
    }

    @Test
    @DisplayName("A block exactly at the threshold is still STANDARD")
    void boundaryIsStandard() {
        assertEquals(WeightClass.STANDARD, classify(number(TIP - THRESHOLD)));
    }

    @Test
    @DisplayName("A block ahead of the tip is STANDARD")
    void futureBlockIsStandard() {
        assertEquals(WeightClass.STANDARD, classify(number(TIP + 500)));
    }

    @Test
    @DisplayName("retrieveLatest, the max block number, and an empty request are STANDARD without reading the tip")
    void latestAndUnclassifiableAreStandard() {
        final AtomicInteger tipReads = new AtomicInteger();
        final GetBlockWeigher counting = new GetBlockWeigher(
                () -> {
                    tipReads.incrementAndGet();
                    return TIP;
                },
                THRESHOLD);
        for (final BlockRequest request : new BlockRequest[] {
            BlockRequest.newBuilder().retrieveLatest(true).build(), number(-1L), BlockRequest.DEFAULT
        }) {
            assertEquals(WeightClass.STANDARD, counting.classify(METHOD, BlockRequest.PROTOBUF.toBytes(request)));
        }
        assertEquals(0, tipReads.get());
    }

    @Test
    @DisplayName("Malformed bytes are STANDARD rather than failing")
    void malformedIsStandard() {
        assertEquals(WeightClass.STANDARD, weigher.classify(METHOD, Bytes.wrap(new byte[] {0x08})));
        assertEquals(WeightClass.STANDARD, weigher.classify(METHOD, Bytes.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF})));
    }
}
