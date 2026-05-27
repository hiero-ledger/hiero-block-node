// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.subscriber;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.api.BlockStreamFilter;
import org.hiero.block.api.SubscribeStreamRequest;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.node.base.filter.BlockItemFilter;
import org.junit.jupiter.api.Test;

/**
 * Plugin-level integration test for STORY-F4: confirms that
 * {@link SubscribeStreamRequest#filter()} drives the same {@link BlockItemFilter}
 * the subscriber session applies on outbound blocks. Exercises the same wiring
 * {@code BlockStreamSubscriberSession.SessionContext.create} uses —
 * request.filter() → {@link BlockItemFilter}.
 */
class SubscribeStreamRequestFilterTest {

    @Test
    void requestWithoutFilterYieldsIdentity() {
        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(0L)
                .endBlockNumber(10L)
                .build();
        final BlockItemFilter filter = BlockItemFilter.from(request.filter());
        assertThat(filter.isIdentity()).isTrue();
    }

    @Test
    void requestWithDenylistDropsConfiguredItemTypes() {
        final SubscribeStreamRequest request = SubscribeStreamRequest.newBuilder()
                .startBlockNumber(0L)
                .endBlockNumber(10L)
                .filter(BlockStreamFilter.newBuilder()
                        .include(false)
                        .blockItemTypes(List.of(ItemOneOfType.TRANSACTION_RESULT.protoOrdinal()))
                        .build())
                .build();
        final BlockItemFilter filter = BlockItemFilter.from(request.filter());

        final List<BlockItemUnparsed> in = List.of(
                BlockItemUnparsed.newBuilder().blockHeader(Bytes.fromHex("01")).build(),
                BlockItemUnparsed.newBuilder().transactionResult(Bytes.fromHex("05")).build(),
                BlockItemUnparsed.newBuilder().blockProof(Bytes.fromHex("09")).build());
        final List<BlockItemUnparsed> out = filter.apply(in);

        assertThat(out).hasSize(3);
        assertThat(out.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER);
        assertThat(out.get(1).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM);
        assertThat(out.get(2).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF);
    }
}
