// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.filter;

import static org.assertj.core.api.Assertions.assertThat;

import com.hedera.hapi.block.stream.FilteredSingleItem;
import com.hedera.hapi.block.stream.SubMerkleTree;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import java.util.List;
import org.hiero.block.api.BlockStreamFilter;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;
import org.junit.jupiter.api.Test;

/**
 * Covers the contract of {@link BlockItemFilter}: identity behaviour, allow/deny
 * semantics, mandatory-item override, SubMerkleTree mapping, and the SHA-384
 * hash put into each emitted {@link FilteredSingleItem}.
 */
class BlockItemFilterTest {

    @Test
    void identityFilterPassesEverythingThrough() {
        final BlockItemFilter filter = BlockItemFilter.from(null);
        assertThat(filter.isIdentity()).isTrue();

        final List<BlockItemUnparsed> items = List.of(blockHeader(), stateChanges(), blockProof());
        assertThat(filter.apply(items)).isSameAs(items);
    }

    @Test
    void emptyFilterListIsIdentity() {
        final BlockItemFilter filter = BlockItemFilter.from(
                BlockStreamFilter.newBuilder().include(true).build());
        assertThat(filter.isIdentity()).isTrue();
    }

    @Test
    void allowlistKeepsOnlyListedKinds() {
        // Allow only headers + proof + footer.
        final BlockItemFilter filter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(true)
                .blockItemTypes(List.of(
                        ItemOneOfType.BLOCK_HEADER.protoOrdinal(),
                        ItemOneOfType.BLOCK_PROOF.protoOrdinal(),
                        ItemOneOfType.BLOCK_FOOTER.protoOrdinal()))
                .build());

        final List<BlockItemUnparsed> in = List.of(blockHeader(), stateChanges(), blockProof());
        final List<BlockItemUnparsed> out = filter.apply(in);

        assertThat(out).hasSize(3);
        assertThat(out.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER);
        assertThat(out.get(1).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM);
        assertThat(out.get(2).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF);
    }

    @Test
    void denylistDropsOnlyListedKinds() {
        final BlockItemFilter filter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(false)
                .blockItemTypes(List.of(ItemOneOfType.STATE_CHANGES.protoOrdinal()))
                .build());

        final List<BlockItemUnparsed> in = List.of(blockHeader(), stateChanges(), transactionResult(), blockProof());
        final List<BlockItemUnparsed> out = filter.apply(in);

        assertThat(out).hasSize(4);
        assertThat(out.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER);
        assertThat(out.get(1).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM);
        assertThat(out.get(2).item().kind()).isEqualTo(ItemOneOfType.TRANSACTION_RESULT);
        assertThat(out.get(3).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF);
    }

    @Test
    void mandatoryItemsAreAlwaysForwarded() {
        // A bogus filter that would otherwise drop the headers.
        final BlockItemFilter filter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(false)
                .blockItemTypes(List.of(
                        ItemOneOfType.BLOCK_HEADER.protoOrdinal(),
                        ItemOneOfType.BLOCK_PROOF.protoOrdinal(),
                        ItemOneOfType.BLOCK_FOOTER.protoOrdinal(),
                        ItemOneOfType.STATE_CHANGES.protoOrdinal()))
                .build());

        final List<BlockItemUnparsed> in = List.of(blockHeader(), stateChanges(), blockProof(), blockFooter());
        final List<BlockItemUnparsed> out = filter.apply(in);

        assertThat(out).hasSize(4);
        assertThat(out.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER);
        assertThat(out.get(1).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM);
        assertThat(out.get(2).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF);
        assertThat(out.get(3).item().kind()).isEqualTo(ItemOneOfType.BLOCK_FOOTER);
    }

    @Test
    void filteredItemCarriesExpectedTreeAndHash() throws Exception {
        final BlockItemFilter filter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(false)
                .blockItemTypes(List.of(
                        ItemOneOfType.STATE_CHANGES.protoOrdinal(), ItemOneOfType.TRACE_DATA.protoOrdinal()))
                .build());

        final BlockItemUnparsed stateChanges = stateChanges();
        final BlockItemUnparsed traceData = traceData();
        final List<BlockItemUnparsed> out = filter.apply(List.of(stateChanges, traceData));

        // Per the explicit table in block_item.proto §95-143:
        //   state_changes (7) → STATE_CHANGE_ITEMS_TREE
        //   trace_data    (11) → TRACE_DATA_ITEMS_TREE
        final FilteredSingleItem stateChangesFiltered = parseFiltered(out.get(0));
        assertThat(stateChangesFiltered.tree()).isEqualTo(SubMerkleTree.STATE_CHANGE_ITEMS_TREE);
        final FilteredSingleItem traceDataFiltered = parseFiltered(out.get(1));
        assertThat(traceDataFiltered.tree()).isEqualTo(SubMerkleTree.TRACE_DATA_ITEMS_TREE);

        // Hash matches SHA-384 of the original bytes.
        final byte[] expected = MessageDigest.getInstance("SHA-384")
                .digest(BlockItemUnparsed.PROTOBUF.toBytes(stateChanges).toByteArray());
        assertThat(stateChangesFiltered.itemHash()).isEqualTo(Bytes.wrap(expected));
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static BlockItemUnparsed blockHeader() {
        return BlockItemUnparsed.newBuilder().blockHeader(Bytes.fromHex("01")).build();
    }

    private static BlockItemUnparsed blockProof() {
        return BlockItemUnparsed.newBuilder().blockProof(Bytes.fromHex("09")).build();
    }

    private static BlockItemUnparsed blockFooter() {
        return BlockItemUnparsed.newBuilder().blockFooter(Bytes.fromHex("0c")).build();
    }

    private static BlockItemUnparsed stateChanges() {
        return BlockItemUnparsed.newBuilder().stateChanges(Bytes.fromHex("07")).build();
    }

    private static BlockItemUnparsed transactionResult() {
        return BlockItemUnparsed.newBuilder().transactionResult(Bytes.fromHex("05")).build();
    }

    private static BlockItemUnparsed traceData() {
        return BlockItemUnparsed.newBuilder().traceData(Bytes.fromHex("0b")).build();
    }

    private static FilteredSingleItem parseFiltered(final BlockItemUnparsed item) throws Exception {
        return FilteredSingleItem.PROTOBUF.parse(item.filteredSingleItem());
    }
}
