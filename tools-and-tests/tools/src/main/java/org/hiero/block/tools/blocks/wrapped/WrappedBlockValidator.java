// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.hiero.block.tools.utils.PrettyPrint.simpleHash;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.util.Arrays;
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
     * @param block the block to validate
     * @param blockNumber the expected block number
     * @param previousBlockHash the hash of the previous block, or null for the first block being validated
     * @param network the network name (e.g., "mainnet", "testnet") for network-specific validation
     * @param streamingHasher the streaming hasher tracking the all-blocks merkle tree, or null if
     *                        merkle tree validation should be skipped
     * @throws ValidationException if the block fails validation
     */
    public static void validateBlock(
            final Block block,
            final long blockNumber,
            final byte[] previousBlockHash,
            final String network,
            final @Nullable StreamingHasher streamingHasher)
            throws ValidationException {
        validateBlockChain(blockNumber, block, previousBlockHash);
        validateHistoricalBlockTreeRoot(blockNumber, block, streamingHasher);
        validateAmendments(blockNumber, block, network);
        validateNoExtraItems(blockNumber, block);
        validate50Billion(blockNumber, block);
    }

    public static void validateBlockChain(
            final long blockNumber, final Block block, final byte[] previousBlockHash)
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

    public static void validateAmendments(final long blockNumber, final Block block, final String network) {
        if (network.equals("mainnet")) {}
    }

    public static void validateNoExtraItems(final long blockNumber, final Block block) throws ValidationException {
        long itemCount = block.items().size();
        if (itemCount > 4) {
            throw new ValidationException("Block: " + blockNumber
                    + " - Block contains extra items. Expected at most 4 items (BlockHeader, RecordFile, BlockFooter, BlockProof), but found "
                    + itemCount);
        }
    }

    public static void validate50Billion(final long blockNumber, final Block block) throws ValidationException {}
}
