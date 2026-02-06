// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.hiero.block.tools.utils.PrettyPrint.simpleHash;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.MapUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.node.base.AccountAmount;
import com.hedera.hapi.streams.RecordStreamItem;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.jspecify.annotations.Nullable;

/**
 * Validates wrapped block stream blocks produced by {@link org.hiero.block.tools.blocks.ToWrappedBlocksCommand}.
 *
 * <p>This class provides static validation methods that can be called from the CLI subcommand
 * {@link ValidateWrappedBlocksCommand} or from any other code that needs to validate wrapped blocks.
 */
public final class WrappedBlockValidator {

    /** Total HBAR supply expressed in tinybar (50 billion HBAR * 100 million tinybar per HBAR). */
    static final long FIFTY_BILLION_HBAR_IN_TINYBAR = 5_000_000_000_000_000_000L;

    /** Private constructor to prevent instantiation. */
    private WrappedBlockValidator() {}

    /**
     * Validates a single wrapped block.
     *
     * <p>When {@code streamingHasher} is non-null, the historical block hash merkle tree is validated
     * by computing the expected all-blocks merkle tree root hash from the hasher's current state.
     * After a successful validation, the block's hash is added to the streaming hasher so it is ready
     * for the next block. When {@code streamingHasher} is null, the all-blocks merkle tree portion
     * of the block hash cannot be validated (e.g. when starting validation from a block other than
     * block zero).
     *
     * <p>When {@code balanceMap} is non-null, account balances are tracked across blocks and the
     * total supply is verified to equal 50 billion HBAR (in tinybar) after each block. The map is
     * mutated in place so the caller can pass the same map across successive blocks. When null,
     * the 50 billion HBAR validation is skipped.
     *
     * @param block the block to validate
     * @param blockNumber the expected block number
     * @param previousBlockHash the hash of the previous block, or null for the first block being validated
     * @param network the network name (e.g., "mainnet", "testnet") for network-specific validation
     * @param streamingHasher the streaming hasher tracking the all-blocks merkle tree, or null if
     *                        merkle tree validation should be skipped
     * @param balanceMap mutable map of account number to tinybar balance, or null to skip supply validation
     * @throws ValidationException if the block fails validation
     */
    public static void validateBlock(
            final Block block,
            final long blockNumber,
            final byte[] previousBlockHash,
            final String network,
            final @Nullable StreamingHasher streamingHasher,
            final @Nullable Map<Long, Long> balanceMap)
            throws ValidationException {
        System.out.println(
                "WrappedBlockValidator.validateBlock " + blockNumber + ": network=" + network + " streamingHasher="
                        + streamingHasher + " balanceMap.size="
                        + (balanceMap == null ? "null" : balanceMap.size()));
        validateBlockChain(blockNumber, block, previousBlockHash);
        validateHistoricalBlockTreeRoot(blockNumber, block, streamingHasher);
        validateAmendments(blockNumber, block, network);
        validateRequiredItems(blockNumber, block);
        validateNoExtraItems(blockNumber, block);
        validate50Billion(blockNumber, block, balanceMap);
    }

    public static void validateBlockChain(final long blockNumber, final Block block, final byte[] previousBlockHash)
            throws ValidationException {
        if (previousBlockHash != null) {
            byte[] readHash = block.items().stream()
                    .filter(BlockItem::hasBlockFooter)
                    .findFirst()
                    .orElseThrow(() -> new ValidationException(
                            "Block: " + blockNumber + "Block footer with historical block tree root not found"))
                    .blockFooterOrThrow()
                    .previousBlockRootHash()
                    .toByteArray();
            if (!Arrays.equals(previousBlockHash, readHash)) {
                throw new ValidationException("Block: " + blockNumber
                        + " - Blockchain is not valid: previous block hash mismatch. " + "previousBlockHash= "
                        + simpleHash(previousBlockHash) + " readHash= " + simpleHash(readHash));
            }
        }
    }

    public static void validateHistoricalBlockTreeRoot(
            final long blockNumber, final Block block, final StreamingHasher streamingHasher)
            throws ValidationException {
        if (streamingHasher != null) {
            byte[] expectedHash = streamingHasher.computeRootHash();
            byte[] readHash = block.items().stream()
                    .filter(BlockItem::hasBlockFooter)
                    .findFirst()
                    .orElseThrow(() -> new ValidationException(
                            "Block: " + blockNumber + "Block footer with historical block tree root not found"))
                    .blockFooterOrThrow()
                    .rootHashOfAllBlockHashesTree()
                    .toByteArray();
            if (!Arrays.equals(expectedHash, readHash)) {
                throw new ValidationException("Block: " + blockNumber + " - Historical block tree root hash mismatch. "
                        + "expectedHash= " + simpleHash(expectedHash) + " readHash= " + simpleHash(readHash));
            }
        }
    }

    @SuppressWarnings({"unused", "StatementWithEmptyBody"})
    public static void validateAmendments(final long blockNumber, final Block block, final String network) {
        if (network.equals("mainnet")) {}
    }

    /**
     * Validates that all minimum required items are present in the block.
     *
     * <p>Every wrapped block must contain at least one of each: {@code BlockHeader},
     * {@code RecordFile}, {@code BlockFooter}, and {@code BlockProof}.
     *
     * @param blockNumber the block number for error reporting
     * @param block the block to validate
     * @throws ValidationException if any required item type is missing
     */
    public static void validateRequiredItems(final long blockNumber, final Block block) throws ValidationException {
        final List<BlockItem> items = block.items();
        if (items.isEmpty()) {
            throw new ValidationException("Block: " + blockNumber + " - Block has no items");
        }
        boolean hasHeader = false;
        boolean hasRecordFile = false;
        boolean hasFooter = false;
        boolean hasProof = false;
        for (BlockItem item : items) {
            if (item.hasBlockHeader()) hasHeader = true;
            if (item.hasRecordFile()) hasRecordFile = true;
            if (item.hasBlockFooter()) hasFooter = true;
            if (item.hasBlockProof()) hasProof = true;
        }
        if (!hasHeader) {
            throw new ValidationException("Block: " + blockNumber + " - Missing required BlockHeader");
        }
        if (!hasRecordFile) {
            throw new ValidationException("Block: " + blockNumber + " - Missing required RecordFile");
        }
        if (!hasFooter) {
            throw new ValidationException("Block: " + blockNumber + " - Missing required BlockFooter");
        }
        if (!hasProof) {
            throw new ValidationException("Block: " + blockNumber + " - Missing required BlockProof");
        }
    }

    /**
     * Validates that no unexpected items are present in the block and that items appear in the
     * correct order.
     *
     * <p>The only valid structure for a wrapped block is:
     *
     * <ol>
     *   <li>Exactly one {@code BlockHeader}
     *   <li>Zero or more {@code StateChanges} (genesis amendments for block 0)
     *   <li>Exactly one {@code RecordFile}
     *   <li>Exactly one {@code BlockFooter}
     *   <li>One or more {@code BlockProof} items
     * </ol>
     *
     * <p>Any item that does not fit this structure is considered unexpected.
     *
     * @param blockNumber the block number for error reporting
     * @param block the block to validate
     * @throws ValidationException if unexpected items are found or items are out of order
     */
    public static void validateNoExtraItems(final long blockNumber, final Block block) throws ValidationException {
        final List<BlockItem> items = block.items();
        final int size = items.size();
        int index = 0;

        // 1. Exactly one BlockHeader
        if (index >= size || !items.get(index).hasBlockHeader()) {
            throw new ValidationException("Block: " + blockNumber + " - First item must be a BlockHeader, found "
                    + describeItem(items, index));
        }
        index++;
        if (index < size && items.get(index).hasBlockHeader()) {
            throw new ValidationException("Block: " + blockNumber + " - Multiple BlockHeaders found at index " + index);
        }

        // 2. Zero or more StateChanges
        while (index < size && items.get(index).hasStateChanges()) {
            index++;
        }

        // 3. Exactly one RecordFile
        if (index >= size || !items.get(index).hasRecordFile()) {
            throw new ValidationException("Block: " + blockNumber
                    + " - Expected RecordFile after BlockHeader/StateChanges at index " + index + ", found "
                    + describeItem(items, index));
        }
        index++;
        if (index < size && items.get(index).hasRecordFile()) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Multiple RecordFile items found at index " + index);
        }

        // 4. Exactly one BlockFooter
        if (index >= size || !items.get(index).hasBlockFooter()) {
            throw new ValidationException("Block: " + blockNumber
                    + " - Expected BlockFooter after RecordFile at index " + index + ", found "
                    + describeItem(items, index));
        }
        index++;
        if (index < size && items.get(index).hasBlockFooter()) {
            throw new ValidationException(
                    "Block: " + blockNumber + " - Multiple BlockFooter items found at index " + index);
        }

        // 5. One or more BlockProof items
        if (index >= size || !items.get(index).hasBlockProof()) {
            throw new ValidationException("Block: " + blockNumber
                    + " - Expected BlockProof after BlockFooter at index " + index + ", found "
                    + describeItem(items, index));
        }
        while (index < size && items.get(index).hasBlockProof()) {
            index++;
        }

        // Should be at the end
        if (index < size) {
            throw new ValidationException("Block: " + blockNumber + " - Unexpected " + describeItem(items, index)
                    + " at index " + index + " after BlockProof(s)");
        }
    }

    /**
     * Returns a human-readable description of the item at the given index, or "end of block" if
     * the index is past the end.
     */
    private static String describeItem(final List<BlockItem> items, final int index) {
        if (index >= items.size()) {
            return "end of block";
        }
        final BlockItem item = items.get(index);
        if (item.hasBlockHeader()) return "BlockHeader";
        if (item.hasRecordFile()) return "RecordFile";
        if (item.hasBlockFooter()) return "BlockFooter";
        if (item.hasBlockProof()) return "BlockProof";
        if (item.hasStateChanges()) return "StateChanges";
        return item.item().kind().name();
    }

    /**
     * Validates that the total HBAR supply equals exactly 50 billion HBAR (in tinybar) after
     * processing this block.
     *
     * <p>Account balances are updated from two sources within the block, processed in item order:
     *
     * <ol>
     *   <li>{@code StateChanges} items – map updates set absolute balances; map deletes remove accounts.
     *   <li>{@code RecordFile} items – transfer lists apply relative balance changes.
     * </ol>
     *
     * <p>After all items are processed the sum of all account balances must equal
     * {@link #FIFTY_BILLION_HBAR_IN_TINYBAR}. When {@code balanceMap} is null, this validation is
     * skipped.
     *
     * @param blockNumber the block number for error reporting
     * @param block the block to validate
     * @param balanceMap mutable map of account number to tinybar balance, or null to skip
     * @throws ValidationException if the total supply does not equal 50 billion HBAR
     */
    public static void validate50Billion(
            final long blockNumber, final Block block, final @Nullable Map<Long, Long> balanceMap)
            throws ValidationException {
        if (balanceMap == null) {
            return;
        }

        // Process block items in order – StateChanges first, then RecordFile
        for (final BlockItem item : block.items()) {
            if (item.hasStateChanges()) {
                for (final StateChange stateChange : item.stateChangesOrThrow().stateChanges()) {
                    if (stateChange.hasMapUpdate()) {
                        final MapUpdateChange mapUpdate = stateChange.mapUpdateOrThrow();
                        final MapChangeKey key = mapUpdate.keyOrThrow();
                        final MapChangeValue value = mapUpdate.valueOrThrow();
                        if (key.hasAccountIdKey() && value.hasAccountValue()) {
                            balanceMap.put(
                                    key.accountIdKeyOrThrow().accountNumOrThrow(),
                                    value.accountValueOrThrow().tinybarBalance());
                        }
                    } else if (stateChange.hasMapDelete()) {
                        final MapChangeKey key = stateChange.mapDeleteOrThrow().keyOrThrow();
                        if (key.hasAccountIdKey()) {
                            balanceMap.remove(key.accountIdKeyOrThrow().accountNumOrThrow());
                        }
                    }
                }
            } else if (item.hasRecordFile()) {
                for (final RecordStreamItem recordStreamItem :
                        item.recordFileOrThrow().recordFileContentsOrThrow().recordStreamItems()) {
                    for (final AccountAmount accountAmount : recordStreamItem
                            .recordOrThrow()
                            .transferListOrThrow()
                            .accountAmounts()) {
                        balanceMap.merge(
                                accountAmount.accountIDOrThrow().accountNumOrThrow(),
                                accountAmount.amount(),
                                Long::sum);
                    }
                }
            }
        }

        // Sum all balances and verify total supply
        long totalBalance = 0;
        for (final long balance : balanceMap.values()) {
            totalBalance += balance;
        }
        if (totalBalance != FIFTY_BILLION_HBAR_IN_TINYBAR) {
            final long difference = totalBalance - FIFTY_BILLION_HBAR_IN_TINYBAR;
            throw new ValidationException("Block: " + blockNumber + " - Total HBAR supply mismatch. Expected "
                    + FIFTY_BILLION_HBAR_IN_TINYBAR + " tinybar but found " + totalBalance + " tinybar (difference: "
                    + difference + ")");
        }
    }
}
