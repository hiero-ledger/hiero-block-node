// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.*;

import com.hedera.hapi.block.stream.experimental.Block;
import com.hedera.hapi.block.stream.experimental.BlockFooter;
import com.hedera.hapi.block.stream.experimental.BlockItem;
import com.hedera.hapi.block.stream.experimental.FilteredItemHash;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import org.hiero.block.tools.utils.Sha384;

/**
 * Computes the root hash of a block following the Block Merkle Tree Design specification.
 *
 * <p>The block root hash is computed from a fixed 16-leaf tree structure at the top level,
 * with various subtrees containing block items. The structure enables efficient Merkle proofs
 * for any data within the block.
 *
 * <h2>Block Root Tree Structure (16 fixed leaves)</h2>
 * <pre>
 *                                     Block Root
 *                                          │
 *                            ┌─────────────┴────────────┐
 *                    Consensus Time              Fixed Root Tree
 *                     (MerkleLeaf)                 (16 leaves)
 *                                                       │
 *                                          ┌────────────┴──────────────┐
 *                                     Left Subtree                  Reserved
 *                                          │                      (future use)
 *                     ┌────────────────────┴───────────────────┐
 *                 Left-Left                                Left-Right
 *                     │                                        │
 *           ┌─────────┴─────────┐                     ┌────────┴────────┐
 *     left-left-left      left-left-right      left-right-left   left-right-right
 *           │                   │                     │                 │
 *     ┌─────┴────┐       ┌──────┴──────┐           ┌──┴──┐       ┌──────┴──────┐
 * PrevBlock  AllBlocks  State  ConsensusHeaders  Input Output  StateChanges  Trace
 * </pre>
 *
 * <h2>Fixed Leaf Positions (from design doc)</h2>
 * <ol>
 *   <li><b>Previous Block Root Hash</b> - Links to previous block, forming the blockchain</li>
 *   <li><b>All Block Hashes Tree Root</b> - Streaming merkle tree of all previous block hashes</li>
 *   <li><b>State Root Hash</b> - State merkle tree root at block start</li>
 *   <li><b>Consensus Headers</b> - BlockHeader, EventHeader, RoundHeader items</li>
 *   <li><b>Input Items</b> - SignedTransaction, RecordFile items</li>
 *   <li><b>Output Items</b> - TransactionResult, TransactionOutput items</li>
 *   <li><b>State Changes</b> - StateChanges items</li>
 *   <li><b>Trace Data</b> - TraceData items</li>
 *   <li>-16. <b>Reserved</b> - For future expansion</li>
 * </ol>
 *
 * <h2>Subtree Item Types</h2>
 * <ul>
 *   <li>Consensus Headers: BLOCK_HEADER, EVENT_HEADER, ROUND_HEADER</li>
 *   <li>Input Items: SIGNED_TRANSACTION, RECORD_FILE</li>
 *   <li>Output Items: TRANSACTION_RESULT, TRANSACTION_OUTPUT</li>
 *   <li>State Changes: STATE_CHANGES</li>
 *   <li>Trace Data: TRACE_DATA</li>
 * </ul>
 *
 * <h2>Special Items (Not Hashed)</h2>
 * <ul>
 *   <li>BLOCK_FOOTER - Contains hashes already included elsewhere in the tree</li>
 *   <li>BLOCK_PROOF - Proves the hash, so cannot be part of it</li>
 * </ul>
 *
 * @see StreamingHasher
 * @see HashingUtils
 */
public class BlockStreamBlockHasher {

    /**
     * Computes the root hash of a block.
     *
     * <p>The computation follows the Block Merkle Tree Design:
     * <ol>
     *   <li>Extract block header and footer for required hashes</li>
     *   <li>Build streaming merkle trees for each item category</li>
     *   <li>Combine into the fixed 16-leaf root structure</li>
     * </ol>
     *
     * @param block the block to hash (must contain BlockHeader and BlockFooter)
     * @param allBlocksMerkleTreeRootHash the root hash of the tree containing all previous block hashes
     * @return the 48-byte SHA-384 block root hash
     * @throws IllegalArgumentException if the block is missing required header or footer
     */
    public static byte[] hashBlock(Block block, byte[] allBlocksMerkleTreeRootHash) {
        // create SHA-384 digest instance for all hashing
        final MessageDigest digest = Sha384.sha384Digest();
        // extract block header and footer
        final BlockHeader blockHeader = block.items().getFirst().blockHeader();
        if (blockHeader == null) {
            throw new IllegalArgumentException("Block is missing BlockHeader");
        }
        final BlockFooter blockFooter = block.items().stream()
                .filter(BlockItem::hasBlockFooter)
                .findFirst()
                .orElseThrow()
                .blockFooter();
        if (blockFooter == null) {
            throw new IllegalArgumentException("Block is missing BlockFooter");
        }
        // get consensus timestamp hash
        final Timestamp consensusTimestamp = blockHeader.blockTimestampOrThrow();
        // get previous block hash
        final byte[] previousBlockHash = blockFooter.previousBlockRootHash().toByteArray();
        // get state root hash, treating missing state root as zero hash
        final byte[] stateRootHash = blockFooter.startOfBlockStateRootHash().length() == 0
                ? EMPTY_TREE_HASH
                : blockFooter.startOfBlockStateRootHash().toByteArray();
        // build streaming merkle trees of items in the block
        final StreamingHasher consensusHeadersHasher = new StreamingHasher();
        final StreamingHasher inputItemsHasher = new StreamingHasher();
        final StreamingHasher outputItemsHasher = new StreamingHasher();
        final StreamingHasher stateChangeItemsHasher = new StreamingHasher();
        final StreamingHasher traceItemsHasher = new StreamingHasher();
        for (BlockItem blockItem : block.items()) {
            // protobuf serialization of item for hashing
            final Bytes blockItemBytes = BlockItem.PROTOBUF.toBytes(blockItem);
            // add the item to the appropriate merkle tree
            switch (blockItem.item().kind()) {
                case EVENT_HEADER, ROUND_HEADER -> consensusHeadersHasher.addLeaf(blockItemBytes);
                case SIGNED_TRANSACTION -> inputItemsHasher.addLeaf(blockItemBytes);
                case BLOCK_HEADER, RECORD_FILE, TRANSACTION_RESULT, TRANSACTION_OUTPUT ->
                    outputItemsHasher.addLeaf(blockItemBytes);
                case STATE_CHANGES -> stateChangeItemsHasher.addLeaf(blockItemBytes);
                case FILTERED_ITEM_HASH -> {
                    FilteredItemHash filteredItemHash = blockItem.filteredItemHashOrThrow();
                    Bytes hash = filteredItemHash.itemHash();
                    // TODO work out which tree filtered item belongs to
                    stateChangeItemsHasher.addLeaf(hash.toByteArray());
                }
                case TRACE_DATA -> traceItemsHasher.addLeaf(blockItemBytes);
                case BLOCK_FOOTER -> {} // not part of any tree as it contains hashes that are elsewhere in the block
                // tree
                case BLOCK_PROOF -> {} // ignore, not hashed as it proves the hash so can't be part of it
            }
        }
        // combine all the merkle tree roots and other block data into final block hash
        // spotless:off
        // Code here won't be formatted by Spotless, Spotless makes it less readable
        return hashInternalNode(digest,
            hashLeaf(digest, Timestamp.PROTOBUF.toBytes(consensusTimestamp).toByteArray()),
            hashInternalNode(digest,
                hashInternalNode(digest,
                    hashInternalNode(digest,
                        hashInternalNode(digest,
                            previousBlockHash,
                            allBlocksMerkleTreeRootHash
                        ),
                        hashInternalNode(digest,
                            stateRootHash,
                            consensusHeadersHasher.computeRootHash()
                        )
                    ),
                    hashInternalNode(digest,
                        hashInternalNode(digest,
                            inputItemsHasher.computeRootHash(),
                            outputItemsHasher.computeRootHash()
                        ),
                        hashInternalNode(digest,
                            stateChangeItemsHasher.computeRootHash(),
                            traceItemsHasher.computeRootHash()
                        )
                    )
                ),
                null // reserved for future use
            )
        );
        // spotless:on
    }
}
