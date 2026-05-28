// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hedera.hapi.block.stream.FilteredSingleItem;
import com.hedera.hapi.block.stream.SubMerkleTree;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.ByteBuffer;
import java.util.List;
import org.hiero.block.api.BlockStreamFilter;
import org.hiero.block.common.hasher.HashingUtilities;
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
    void emptyDenylistIsIdentity() {
        final BlockItemFilter filter = BlockItemFilter.from(
                BlockStreamFilter.newBuilder().include(false).build());
        assertThat(filter.isIdentity()).isTrue();
    }

    @Test
    void allowlistKeepsOnlyListedKinds() {
        // Allow only TRACE_DATA (11). BLOCK_HEADER, BLOCK_PROOF, BLOCK_FOOTER are
        // mandatory and pass through regardless; STATE_CHANGES is not on the
        // allowlist so it gets replaced with a FilteredSingleItem.
        final BlockItemFilter filter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(true)
                .blockItemTypes(List.of(ItemOneOfType.TRACE_DATA.protoOrdinal()))
                .build());

        final List<BlockItemUnparsed> in = List.of(blockHeader(), stateChanges(), traceData(), blockProof());
        final List<BlockItemUnparsed> out = filter.apply(in);

        assertThat(out).hasSize(4);
        assertThat(out.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER); // mandatory
        assertThat(out.get(1).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM); // not on allowlist
        assertThat(out.get(2).item().kind()).isEqualTo(ItemOneOfType.TRACE_DATA); // allow-listed
        assertThat(out.get(3).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF); // mandatory
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
    void mandatoryItemsAreAlwaysForwardedEvenWhenAllowlistOmitsThem() {
        // Allow only STATE_CHANGES — BLOCK_HEADER/PROOF/FOOTER are not in the
        // allowlist but must still pass because they are required by the proof.
        final BlockItemFilter filter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(true)
                .blockItemTypes(List.of(ItemOneOfType.STATE_CHANGES.protoOrdinal()))
                .build());

        final List<BlockItemUnparsed> in = List.of(blockHeader(), stateChanges(), blockProof(), blockFooter());
        final List<BlockItemUnparsed> out = filter.apply(in);

        assertThat(out).hasSize(4);
        assertThat(out.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER);
        assertThat(out.get(1).item().kind()).isEqualTo(ItemOneOfType.STATE_CHANGES);
        assertThat(out.get(2).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF);
        assertThat(out.get(3).item().kind()).isEqualTo(ItemOneOfType.BLOCK_FOOTER);
    }

    @Test
    void fromRejectsUnsupportedFilterTargets() {
        // Mandatory items (1, 9, 12) and filter markers (8, 19) are not valid filter
        // targets — they may not appear in block_item_types regardless of include flag.
        for (final int unsupported : List.of(
                ItemOneOfType.BLOCK_HEADER.protoOrdinal(),
                ItemOneOfType.FILTERED_SINGLE_ITEM.protoOrdinal(),
                ItemOneOfType.BLOCK_PROOF.protoOrdinal(),
                ItemOneOfType.BLOCK_FOOTER.protoOrdinal(),
                ItemOneOfType.REDACTED_ITEM.protoOrdinal())) {
            final BlockStreamFilter proto = BlockStreamFilter.newBuilder()
                    .include(false)
                    .blockItemTypes(List.of(unsupported))
                    .build();
            assertThatThrownBy(() -> BlockItemFilter.from(proto))
                    .as("filter target " + unsupported + " should be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(String.valueOf(unsupported));
        }
    }

    @Test
    void fromRejectsIncludeTrueWithEmptyTypes() {
        // include=true + empty list = "allow nothing" = deny everything. The caller
        // almost certainly didn't mean that, so we reject rather than silently
        // degrading to identity.
        final BlockStreamFilter proto =
                BlockStreamFilter.newBuilder().include(true).build();
        assertThatThrownBy(() -> BlockItemFilter.from(proto))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-empty");
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

        // item_hash matches the leaf hash the verifier would compute for the
        // original item (LEAF_PREFIX || item_bytes, SHA-384). Same source of
        // truth as HashingUtilities.getBlockItemHash — required for filtered
        // blocks to still satisfy the block proof.
        final ByteBuffer expected = HashingUtilities.getBlockItemHash(stateChanges);
        assertThat(stateChangesFiltered.itemHash()).isEqualTo(Bytes.wrap(expected.array()));
    }

    @Test
    void allSupportedFilterTargetsMapToExpectedSubMerkleTree() throws Exception {
        // For every block-item kind a client may filter, assert the
        // FilteredSingleItem.tree it produces. Covers the explicit field→subtree
        // table from block_item.proto §95-143 across the supported filter set.
        record Case(ItemOneOfType kind, BlockItemUnparsed item, SubMerkleTree expectedTree) {}
        final List<Case> cases = List.of(
                new Case(ItemOneOfType.EVENT_HEADER, eventHeader(), SubMerkleTree.CONSENSUS_HEADER_ITEMS),
                new Case(ItemOneOfType.ROUND_HEADER, roundHeader(), SubMerkleTree.CONSENSUS_HEADER_ITEMS),
                new Case(ItemOneOfType.SIGNED_TRANSACTION, signedTransaction(), SubMerkleTree.INPUT_ITEMS_TREE),
                new Case(ItemOneOfType.TRANSACTION_RESULT, transactionResult(), SubMerkleTree.OUTPUT_ITEMS_TREE),
                new Case(ItemOneOfType.TRANSACTION_OUTPUT, transactionOutput(), SubMerkleTree.OUTPUT_ITEMS_TREE),
                new Case(ItemOneOfType.STATE_CHANGES, stateChanges(), SubMerkleTree.STATE_CHANGE_ITEMS_TREE),
                new Case(ItemOneOfType.RECORD_FILE, recordFile(), SubMerkleTree.OUTPUT_ITEMS_TREE),
                new Case(ItemOneOfType.TRACE_DATA, traceData(), SubMerkleTree.TRACE_DATA_ITEMS_TREE));

        for (final Case c : cases) {
            final BlockItemFilter filter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                    .include(false)
                    .blockItemTypes(List.of(c.kind.protoOrdinal()))
                    .build());
            final List<BlockItemUnparsed> out = filter.apply(List.of(c.item));
            assertThat(out.get(0).item().kind())
                    .as("filtered output for %s", c.kind)
                    .isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM);
            assertThat(parseFiltered(out.get(0)).tree())
                    .as("SubMerkleTree mapping for %s", c.kind)
                    .isEqualTo(c.expectedTree);
        }
    }

    @Test
    void publisherFilterComposesWithSubscriberFilterWithoutDoubleFiltering() {
        // Compose two filters back-to-back to mirror the runtime path:
        // publisher (ingress) → subscriber (egress). Publisher drops STATE_CHANGES;
        // subscriber drops TRACE_DATA. Mandatory items must survive both filters,
        // and a publisher-filtered slot must not be re-filtered by the subscriber.
        final BlockItemFilter publisherFilter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(false)
                .blockItemTypes(List.of(ItemOneOfType.STATE_CHANGES.protoOrdinal()))
                .build());
        final BlockItemFilter subscriberFilter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(false)
                .blockItemTypes(List.of(ItemOneOfType.TRACE_DATA.protoOrdinal()))
                .build());

        final List<BlockItemUnparsed> raw = List.of(
                blockHeader(), stateChanges(), traceData(), transactionResult(), blockProof(), blockFooter());
        final List<BlockItemUnparsed> afterPublisher = publisherFilter.apply(raw);
        final List<BlockItemUnparsed> afterSubscriber = subscriberFilter.apply(afterPublisher);

        // Position-by-position assertions on the composed output.
        assertThat(afterSubscriber).hasSize(6);
        assertThat(afterSubscriber.get(0).item().kind()).isEqualTo(ItemOneOfType.BLOCK_HEADER);
        assertThat(afterSubscriber.get(1).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM); // by publisher
        assertThat(afterSubscriber.get(2).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM); // by subscriber
        assertThat(afterSubscriber.get(3).item().kind()).isEqualTo(ItemOneOfType.TRANSACTION_RESULT); // untouched
        assertThat(afterSubscriber.get(4).item().kind()).isEqualTo(ItemOneOfType.BLOCK_PROOF);
        assertThat(afterSubscriber.get(5).item().kind()).isEqualTo(ItemOneOfType.BLOCK_FOOTER);
    }

    @Test
    void subMerkleTreeOfThrowsForUnmappedFieldNumber() {
        // The mapping table only covers SUPPORTED_FILTER_TYPES — anything else
        // (mandatory items, filter markers, hypothetical future variants) must
        // fail loudly so the cause is obvious if SUPPORTED_FILTER_TYPES is ever
        // widened without updating the table.
        for (final int unmapped : List.of(0, 1, 8, 9, 12, 13, 19, 20, 99)) {
            assertThatThrownBy(() -> BlockItemFilter.subMerkleTreeOf(unmapped))
                    .as("field number %d should have no SubMerkleTree mapping", unmapped)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(String.valueOf(unmapped));
        }
    }

    @Test
    void composingDenylistsForSameKindDoesNotDoubleFilter() {
        // Publisher and subscriber both deny STATE_CHANGES. The subscriber filter
        // must see the publisher's FilteredSingleItem and pass it through
        // unchanged (FilteredSingleItem is in ALWAYS_FORWARD), so the output is
        // identical to the publisher's output — no recursive replacement.
        final BlockItemFilter publisherFilter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(false)
                .blockItemTypes(List.of(ItemOneOfType.STATE_CHANGES.protoOrdinal()))
                .build());
        final BlockItemFilter subscriberFilter = BlockItemFilter.from(BlockStreamFilter.newBuilder()
                .include(false)
                .blockItemTypes(List.of(ItemOneOfType.STATE_CHANGES.protoOrdinal()))
                .build());

        final List<BlockItemUnparsed> raw = List.of(blockHeader(), stateChanges(), blockProof());
        final List<BlockItemUnparsed> afterPublisher = publisherFilter.apply(raw);
        final List<BlockItemUnparsed> afterSubscriber = subscriberFilter.apply(afterPublisher);

        assertThat(afterSubscriber)
                .as("subscriber must not re-filter a FilteredSingleItem the publisher produced")
                .containsExactlyElementsOf(afterPublisher);
        assertThat(afterSubscriber.get(1).item().kind()).isEqualTo(ItemOneOfType.FILTERED_SINGLE_ITEM);
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

    private static BlockItemUnparsed eventHeader() {
        return BlockItemUnparsed.newBuilder().eventHeader(Bytes.fromHex("02")).build();
    }

    private static BlockItemUnparsed roundHeader() {
        return BlockItemUnparsed.newBuilder().roundHeader(Bytes.fromHex("03")).build();
    }

    private static BlockItemUnparsed signedTransaction() {
        return BlockItemUnparsed.newBuilder().signedTransaction(Bytes.fromHex("04")).build();
    }

    private static BlockItemUnparsed transactionOutput() {
        return BlockItemUnparsed.newBuilder().transactionOutput(Bytes.fromHex("06")).build();
    }

    private static BlockItemUnparsed recordFile() {
        return BlockItemUnparsed.newBuilder().recordFile(Bytes.fromHex("0a")).build();
    }

    private static FilteredSingleItem parseFiltered(final BlockItemUnparsed item) throws Exception {
        return FilteredSingleItem.PROTOBUF.parse(item.filteredSingleItem());
    }
}
