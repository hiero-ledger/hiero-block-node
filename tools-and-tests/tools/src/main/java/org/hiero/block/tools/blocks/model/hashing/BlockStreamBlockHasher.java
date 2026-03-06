// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.hashInternalNode;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.hashLeaf;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.FilteredSingleItem;
import com.hedera.hapi.block.stream.output.BlockFooter;
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
 *   <li><b>Previous Block Root Hash</b> - Links to previous block, forming the blockchain(in the BlockFooter)</li>
 *   <li><b>All Block Hashes Tree Root</b> - Streaming merkle tree of all previous block hashes(in the BlockFooter)</li>
 *   <li><b>State Root Hash</b> - State merkle tree root at block start(in the BlockFooter)</li>
 *   <li><b>Consensus Headers</b> - EventHeader, RoundHeader</li>
 *   <li><b>Input Items</b> - SignedTransaction</li>
 *   <li><b>Output Items</b> - BlockHeader, RecordFileItem, TransactionResult, TransactionOutput items</li>
 *   <li><b>State Changes</b> - StateChanges items</li>
 *   <li><b>Trace Data</b> - TraceData items</li>
 *   <li> Subtrees 9 though 16. <b>Reserved</b> - For future expansion</li>
 * </ol>
 * <p>FilteredSingleItem and RedactedItem are special items types. They can represent any item type, they contain a
 * hash and an enum. The enum states which tree the hash belongs in.</p>
 * <p>BlockFooter is not included in the block hash directly as it is a container for the first 3 hashes PrevBlock,
 * AllBlocks and State of the block root merkle tree.</p>
 * <p>BlockProof is also a special item and is not part of the block hash as it contains a cryptographic proof of the
 * block hash. There can be one or more BlockProof items at the end of the block each proving the block contents in a
 * different cryptographic way</p>
 *
 * @see StreamingHasher
 * @see HashingUtils
 */
public class BlockStreamBlockHasher {

    /**
     * Computes the root hash of a block. Using the `rootHashOfAllBlockHashesTree` from the block footer.
     *
     * <p>The computation follows the Block Merkle Tree Design:
     * <ol>
     *   <li>Extract block header and footer for required hashes</li>
     *   <li>Build streaming merkle trees for each item category</li>
     *   <li>Combine into the fixed 16-leaf root structure</li>
     * </ol>
     *
     * @param block the block to hash (must contain BlockHeader and BlockFooter)
     * @return the 48-byte SHA-384 block root hash
     * @throws IllegalArgumentException if the block is missing the required header or footer
     */
    public static byte[] hashBlock(Block block) {
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
                case FILTERED_SINGLE_ITEM -> {
                    FilteredSingleItem filteredItem = blockItem.filteredSingleItemOrThrow();
                    Bytes hash = filteredItem.itemHash();
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
                            blockFooter.rootHashOfAllBlockHashesTree().toByteArray()
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
