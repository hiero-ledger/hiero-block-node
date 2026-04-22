// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.model.hashing;

import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.hashInternalNode;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.hashLeaf;
import static org.hiero.block.tools.blocks.validation.ProtobufParsingConstants.MAX_PARSE_SIZE;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.hashing.WritableMessageDigest;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.security.MessageDigest;
import org.hiero.block.internal.BlockItemUnparsed;
import org.hiero.block.internal.BlockUnparsed;
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
     * Computes the root hash of a fully-parsed {@link Block} by re-serializing to bytes
     * and delegating to {@link #hashBlock(BlockUnparsed)}.
     *
     * <p>This overload exists for callers that already have a fully-parsed Block (e.g. the
     * wrap command). For validation hot paths, prefer the {@link BlockUnparsed} overload.
     *
     * @param block the fully-parsed block to hash
     * @return the 48-byte SHA-384 block root hash
     */
    public static byte[] hashBlock(Block block) {
        try {
            Bytes bytes = Block.PROTOBUF.toBytes(block);
            BlockUnparsed unparsed = BlockUnparsed.PROTOBUF.parse(
                    bytes.toReadableSequentialData(), false, false, Codec.DEFAULT_MAX_DEPTH, MAX_PARSE_SIZE);
            return hashBlock(unparsed);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to hash block", e);
        }
    }

    /**
     * Computes the root hash of a block using {@link BlockUnparsed} with zero-copy hashing.
     *
     * <p>Uses {@link WritableMessageDigest} to feed protobuf-encoded block items directly into
     * the SHA-384 digest, avoiding intermediate byte[] allocations for each item.
     *
     * <p>Only the BlockHeader and BlockFooter are selectively parsed (for timestamps and
     * fixed-position hashes); all other items are hashed directly from their raw bytes.
     *
     * @param block the unparsed block to hash (must contain BlockHeader and BlockFooter)
     * @return the 48-byte SHA-384 block root hash
     * @throws IllegalArgumentException if the block is missing the required header or footer
     */
    public static byte[] hashBlock(BlockUnparsed block) {
        try {
            return hashBlockInternal(block);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to hash block", e);
        }
    }

    /** Reusable varint encoding buffer (max 5 bytes for a 32-bit varint). */
    private static final ThreadLocal<byte[]> VARINT_BUF = ThreadLocal.withInitial(() -> new byte[5]);

    private static byte[] hashBlockInternal(BlockUnparsed block) throws Exception {
        // create SHA-384 digest instance for all hashing
        final MessageDigest digest = Sha384.sha384Digest();
        // selectively parse block header from the first item's raw bytes
        final BlockItemUnparsed firstItem = block.blockItems().getFirst();
        if (!firstItem.hasBlockHeader()) {
            throw new IllegalArgumentException("Block is missing BlockHeader");
        }
        final BlockHeader blockHeader = BlockHeader.PROTOBUF.parse(firstItem.blockHeaderOrThrow());
        // find and selectively parse block footer
        BlockFooter blockFooter = null;
        for (final BlockItemUnparsed item : block.blockItems()) {
            if (item.hasBlockFooter()) {
                blockFooter = BlockFooter.PROTOBUF.parse(item.blockFooterOrThrow());
                break;
            }
        }
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
        for (final BlockItemUnparsed blockItem : block.blockItems()) {
            switch (blockItem.item().kind()) {
                case EVENT_HEADER, ROUND_HEADER -> {
                    // Feed leaf prefix + protobuf-encoded item directly into digest (zero-copy)
                    digest.update(HashingUtils.LEAF_PREFIX);
                    hashBlockItemDirect(digest, blockItem);
                    consensusHeadersHasher.addNodeByHash(digest.digest());
                }
                case SIGNED_TRANSACTION -> {
                    digest.update(HashingUtils.LEAF_PREFIX);
                    hashBlockItemDirect(digest, blockItem);
                    inputItemsHasher.addNodeByHash(digest.digest());
                }
                case BLOCK_HEADER, RECORD_FILE, TRANSACTION_RESULT, TRANSACTION_OUTPUT -> {
                    digest.update(HashingUtils.LEAF_PREFIX);
                    hashBlockItemDirect(digest, blockItem);
                    outputItemsHasher.addNodeByHash(digest.digest());
                }
                case STATE_CHANGES -> {
                    digest.update(HashingUtils.LEAF_PREFIX);
                    hashBlockItemDirect(digest, blockItem);
                    stateChangeItemsHasher.addNodeByHash(digest.digest());
                }
                case FILTERED_SINGLE_ITEM -> {
                    // Filtered items contain a pre-computed hash; parse to extract it
                    final var filteredItem = com.hedera.hapi.block.stream.FilteredSingleItem.PROTOBUF.parse(
                            blockItem.filteredSingleItemOrThrow());
                    Bytes hash = filteredItem.itemHash();
                    switch (filteredItem.tree()) {
                        case CONSENSUS_HEADER_ITEMS -> consensusHeadersHasher.addNodeByHash(hash.toByteArray());
                        case INPUT_ITEMS_TREE -> inputItemsHasher.addNodeByHash(hash.toByteArray());
                        case OUTPUT_ITEMS_TREE -> outputItemsHasher.addNodeByHash(hash.toByteArray());
                        case STATE_CHANGE_ITEMS_TREE -> stateChangeItemsHasher.addNodeByHash(hash.toByteArray());
                        case TRACE_DATA_ITEMS_TREE -> traceItemsHasher.addNodeByHash(hash.toByteArray());
                    }
                }
                case TRACE_DATA -> {
                    digest.update(HashingUtils.LEAF_PREFIX);
                    hashBlockItemDirect(digest, blockItem);
                    traceItemsHasher.addNodeByHash(digest.digest());
                }
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

    /**
     * Feeds a BlockItemUnparsed directly into the digest by manually writing the protobuf
     * tag + length + payload, bypassing the PBJ codec's write() method and its associated
     * object allocations.
     *
     * <p>This produces the exact same bytes as {@code BlockItemUnparsed.PROTOBUF.write(item, wmd)}
     * because each BlockItemUnparsed has exactly one oneof field containing raw Bytes.
     */
    private static void hashBlockItemDirect(final MessageDigest digest, final BlockItemUnparsed item) {
        final Bytes payload = item.item().as();
        final int fieldNumber = fieldNumberForKind(item.item().kind());
        // Write tag: (fieldNumber << 3) | 2 (wire type LEN)
        writeVarIntToDigest(digest, (fieldNumber << 3) | 2);
        // Write length
        writeVarIntToDigest(digest, (int) payload.length());
        // Write payload bytes directly (zero-copy)
        payload.writeTo(digest);
    }

    /**
     * Maps a BlockItemUnparsed OneOfType to its protobuf field number.
     */
    private static int fieldNumberForKind(final BlockItemUnparsed.ItemOneOfType kind) {
        return switch (kind) {
            case BLOCK_HEADER -> 1;
            case EVENT_HEADER -> 2;
            case ROUND_HEADER -> 3;
            case SIGNED_TRANSACTION -> 4;
            case TRANSACTION_RESULT -> 5;
            case TRANSACTION_OUTPUT -> 6;
            case STATE_CHANGES -> 7;
            case FILTERED_SINGLE_ITEM -> 8;
            case BLOCK_PROOF -> 9;
            case RECORD_FILE -> 10;
            case TRACE_DATA -> 11;
            case BLOCK_FOOTER -> 12;
            case REDACTED_ITEM -> 19;
            case UNSET -> throw new IllegalArgumentException("UNSET block item kind");
        };
    }

    /**
     * Writes a varint-encoded integer directly to a MessageDigest using a thread-local buffer.
     */
    private static void writeVarIntToDigest(final MessageDigest digest, int value) {
        final byte[] buf = VARINT_BUF.get();
        int pos = 0;
        while ((value & ~0x7F) != 0) {
            buf[pos++] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        buf[pos++] = (byte) value;
        digest.update(buf, 0, pos);
    }
}
