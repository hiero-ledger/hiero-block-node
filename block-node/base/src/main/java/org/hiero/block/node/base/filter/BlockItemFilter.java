// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base.filter;

import com.hedera.hapi.block.stream.FilteredSingleItem;
import com.hedera.hapi.block.stream.SubMerkleTree;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hiero.block.api.BlockStreamFilter;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockItemUnparsed.ItemOneOfType;

/**
 * Applies a {@link BlockStreamFilter} to a list of {@link BlockItemUnparsed}
 * items, replacing rejected items with a {@link FilteredSingleItem} carrying
 * the original item's hash and the {@link SubMerkleTree} slot it occupied.
 *
 * <p>The helper is stateless, immutable, and shared by every plugin that
 * filters block items (publisher ingress, subscriber outbound, future
 * block-access reads).
 *
 * <p>Three block-item kinds are <b>always forwarded</b> regardless of the
 * filter — {@code BlockHeader} (1), {@code BlockProof} (9), and
 * {@code BlockFooter} (12) — because the block proof tree requires them.
 */
public final class BlockItemFilter {

    /** Hash algorithm used to populate {@code FilteredSingleItem.item_hash}. */
    private static final String HASH_ALG = "SHA-384";

    /**
     * Block-item kinds that may never be filtered out. The first three are
     * load-bearing for the block proof; the last two are filter markers
     * (we don't re-filter something already filtered or redacted).
     */
    private static final Set<Integer> ALWAYS_FORWARD = Set.of(
            ItemOneOfType.BLOCK_HEADER.protoOrdinal(),
            ItemOneOfType.BLOCK_PROOF.protoOrdinal(),
            ItemOneOfType.BLOCK_FOOTER.protoOrdinal(),
            ItemOneOfType.FILTERED_SINGLE_ITEM.protoOrdinal(),
            ItemOneOfType.REDACTED_ITEM.protoOrdinal());

    /** True ⇒ allowlist (only listed kinds pass); false ⇒ denylist. */
    private final boolean include;

    /** Set of {@code BlockItem.item} oneof field numbers the filter applies to. */
    private final Set<Integer> itemTypes;

    /** Identity filter — passes every item through unchanged. */
    private static final BlockItemFilter IDENTITY = new BlockItemFilter(false, Set.of());

    private BlockItemFilter(final boolean include, @NonNull final Set<Integer> itemTypes) {
        this.include = include;
        this.itemTypes = Collections.unmodifiableSet(new HashSet<>(itemTypes));
    }

    /**
     * Build a filter from the on-wire proto. Returns the identity filter when
     * {@code proto} is null or its {@code block_item_types} list is empty.
     */
    @NonNull
    public static BlockItemFilter from(@Nullable final BlockStreamFilter proto) {
        if (proto == null || proto.blockItemTypes() == null || proto.blockItemTypes().isEmpty()) {
            return IDENTITY;
        }
        return new BlockItemFilter(proto.include(), new HashSet<>(proto.blockItemTypes()));
    }

    /** @return {@code true} if this filter is the identity (pass-through). */
    public boolean isIdentity() {
        return itemTypes.isEmpty();
    }

    /**
     * Decide whether the filter forwards the given item unchanged.
     *
     * @return {@code true} if the item should pass through; {@code false}
     *     if it should be replaced by a {@link FilteredSingleItem}.
     */
    public boolean accepts(@NonNull final BlockItemUnparsed item) {
        if (isIdentity()) {
            return true;
        }
        final int kind = item.item().kind().protoOrdinal();
        if (ALWAYS_FORWARD.contains(kind)) {
            return true;
        }
        final boolean listed = itemTypes.contains(kind);
        return include == listed; // allowlist: pass iff listed; denylist: pass iff NOT listed
    }

    /**
     * Apply the filter to a list of items. Returns a new list; the input is
     * never mutated.
     *
     * <p>Items the filter rejects are replaced by a {@link FilteredSingleItem}
     * carrying the SHA-384 of the original item's PBJ-encoded bytes and the
     * {@link SubMerkleTree} that block-item type maps to.
     */
    @NonNull
    public List<BlockItemUnparsed> apply(@NonNull final List<BlockItemUnparsed> items) {
        if (isIdentity()) {
            return items;
        }
        final List<BlockItemUnparsed> out = new ArrayList<>(items.size());
        for (final BlockItemUnparsed item : items) {
            if (accepts(item)) {
                out.add(item);
            } else {
                out.add(replaceWithFiltered(item));
            }
        }
        return out;
    }

    /**
     * Map a {@code BlockItem.item} oneof field number to its block-proof
     * {@link SubMerkleTree} slot. Follows the rules in
     * {@code block_item.proto:95-143}:
     *
     * <ul>
     *   <li>Fields 1–19 use the explicit "Initial Field assignment to
     *       subtree categories" table in the proto comment.</li>
     *   <li>Fields ≥ 20 use the mod-10 convention (0=CONSENSUS_HEADERS,
     *       1=INPUTS, 2=OUTPUTS, 3=STATE_CHANGES, 4=TRACE_DATA, 9=NOT_HASHED).</li>
     * </ul>
     *
     * <p>Visible for testing.
     */
    @NonNull
    static SubMerkleTree subMerkleTreeOf(final int oneofFieldNumber) {
        return switch (oneofFieldNumber) {
            case 1 -> SubMerkleTree.OUTPUT_ITEMS_TREE; // block_header
            case 2, 3 -> SubMerkleTree.CONSENSUS_HEADER_ITEMS; // event_header, round_header
            case 4 -> SubMerkleTree.INPUT_ITEMS_TREE; // signed_transaction
            case 5, 6, 10 -> SubMerkleTree.OUTPUT_ITEMS_TREE; // transaction_result, transaction_output, record_file
            case 7 -> SubMerkleTree.STATE_CHANGE_ITEMS_TREE; // state_changes
            case 11 -> SubMerkleTree.TRACE_DATA_ITEMS_TREE; // trace_data
            // 8 filtered_single_item, 9 block_proof, 12 block_footer, 19 redacted_item are in
            // ALWAYS_FORWARD and never reach this path.
            default -> switch (oneofFieldNumber % 10) {
                case 0 -> SubMerkleTree.CONSENSUS_HEADER_ITEMS;
                case 1 -> SubMerkleTree.INPUT_ITEMS_TREE;
                case 2 -> SubMerkleTree.OUTPUT_ITEMS_TREE;
                case 3 -> SubMerkleTree.STATE_CHANGE_ITEMS_TREE;
                case 4 -> SubMerkleTree.TRACE_DATA_ITEMS_TREE;
                default -> SubMerkleTree.ITEM_TYPE_UNSPECIFIED;
            };
        };
    }

    @NonNull
    private static BlockItemUnparsed replaceWithFiltered(@NonNull final BlockItemUnparsed item) {
        final int kind = item.item().kind().protoOrdinal();
        final Bytes itemBytes = BlockItemUnparsed.PROTOBUF.toBytes(item);
        final Bytes itemHash = sha384(itemBytes);
        final FilteredSingleItem filteredItem = FilteredSingleItem.newBuilder()
                .itemHash(itemHash)
                .tree(subMerkleTreeOf(kind))
                .build();
        return BlockItemUnparsed.newBuilder()
                .filteredSingleItem(FilteredSingleItem.PROTOBUF.toBytes(filteredItem))
                .build();
    }

    @NonNull
    private static Bytes sha384(@NonNull final Bytes input) {
        try {
            final MessageDigest md = MessageDigest.getInstance(HASH_ALG);
            md.update(input.toByteArray());
            return Bytes.wrap(md.digest());
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(HASH_ALG + " not available", e);
        }
    }
}
