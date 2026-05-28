// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.stream.publisher;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.api.BlockStreamFilter;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.hiero.block.node.base.filter.BlockItemFilter;
import org.junit.jupiter.api.Test;

/**
 * Plugin-level integration test for STORY-F3: confirms that the values on
 * {@link PublisherConfig} drive a {@link BlockItemFilter} that matches the
 * publisher's expected drop / keep behaviour. Exercises the same wiring that
 * {@code PublisherHandler}'s constructor uses — config primitives →
 * {@link BlockStreamFilter} → {@link BlockItemFilter}.
 */
class PublisherConfigFilterTest {

    @Test
    void emptyConfigYieldsIdentityFilter() {
        final PublisherConfig config = defaultConfig(false, List.of());
        final BlockItemFilter filter = filterFromConfig(config);
        assertThat(filter.isIdentity()).isTrue();

        final List<BlockItemUnparsed> in = sampleBlock();
        assertThat(filter.apply(in)).isSameAs(in);
    }

    @Test
    void denylistDropsConfiguredItemTypes() {
        final PublisherConfig config = defaultConfig(false, List.of(ItemOneOfType.STATE_CHANGES.protoOrdinal()));
        final BlockItemFilter filter = filterFromConfig(config);

        final List<BlockItemUnparsed> in = sampleBlock();
        final List<BlockItemUnparsed> out = filter.apply(in);

        assertThat(out).hasSize(3);
        assertThat(out.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER);
        assertThat(out.get(1).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM);
        assertThat(out.get(2).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF);
    }

    @Test
    void allowlistKeepsOnlyConfiguredItemTypes() {
        // Allowlist with only state_changes; mandatory items still pass.
        final PublisherConfig config = defaultConfig(true, List.of(ItemOneOfType.STATE_CHANGES.protoOrdinal()));
        final BlockItemFilter filter = filterFromConfig(config);

        final List<BlockItemUnparsed> in = sampleBlock();
        final List<BlockItemUnparsed> out = filter.apply(in);

        assertThat(out).hasSize(3);
        assertThat(out.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER); // mandatory
        assertThat(out.get(1).item().kind()).isEqualTo(ItemOneOfType.STATE_CHANGES); // allow-listed
        assertThat(out.get(2).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF); // mandatory
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static PublisherConfig defaultConfig(final boolean include, final List<Integer> blockItemTypes) {
        return new PublisherConfig(
                Long.MAX_VALUE,
                300L,
                3,
                100L,
                100_000_000L,
                5,
                include,
                blockItemTypes);
    }

    /** Same builder path PublisherHandler uses internally. */
    private static BlockItemFilter filterFromConfig(final PublisherConfig config) {
        return BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(config.blockStreamFilterInclude())
                .blockItemTypes(config.blockStreamFilterItemTypes())
                .build());
    }

    private static List<BlockItemUnparsed> sampleBlock() {
        return List.of(
                BlockItemUnparsed.newBuilder().blockHeader(Bytes.fromHex("01")).build(),
                BlockItemUnparsed.newBuilder().stateChanges(Bytes.fromHex("07")).build(),
                BlockItemUnparsed.newBuilder().blockProof(Bytes.fromHex("09")).build());
    }
}
