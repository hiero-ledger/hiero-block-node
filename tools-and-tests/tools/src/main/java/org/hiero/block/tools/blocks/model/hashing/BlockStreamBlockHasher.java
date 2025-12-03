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

public class BlockStreamBlockHasher {

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
        // compute consensus timestamp hash
        final Timestamp consensusTimestamp = blockHeader.blockTimestampOrThrow();
        Timestamp.PROTOBUF.toBytes(consensusTimestamp).writeTo(digest);
        final byte[] consensusTimestampHash = digest.digest();
        // get previous block hash
        final byte[] previousBlockHash = blockFooter.previousBlockRootHash().toByteArray();
        // get state root hash, treating missing state root as zero hash
        final byte[] stateRootHash = blockFooter.startOfBlockStateRootHash().length() == 0
                ? Sha384.ZERO_HASH
                : blockFooter.startOfBlockStateRootHash().toByteArray();
        // build streaming merkle trees of items in the block
        final StreamingHasher consensusHeadersHasher = new StreamingHasher();
        final StreamingHasher inputItemsHasher = new StreamingHasher();
        final StreamingHasher outputItemsHasher = new StreamingHasher();
        final StreamingHasher stateChangeItemsHasher = new StreamingHasher();
        final StreamingHasher traceItemsHasher = new StreamingHasher();
        for (BlockItem blockItem : block.items()) {
            // compute hash of the item
            final Bytes itemBytes = BlockItem.PROTOBUF.toBytes(blockItem);
            itemBytes.writeTo(digest);
            final byte[] itemHash = digest.digest();
            // add the item to the appropriate merkle tree
            switch (blockItem.item().kind()) {
                case BLOCK_HEADER, EVENT_HEADER, ROUND_HEADER -> consensusHeadersHasher.addLeaf(itemHash);
                case SIGNED_TRANSACTION, RECORD_FILE -> inputItemsHasher.addLeaf(itemHash);
                case TRANSACTION_RESULT, TRANSACTION_OUTPUT -> outputItemsHasher.addLeaf(itemHash);
                case STATE_CHANGES -> stateChangeItemsHasher.addLeaf(itemHash);
                case FILTERED_ITEM_HASH -> {
                    FilteredItemHash filteredItemHash = blockItem.filteredItemHashOrThrow();
                    Bytes hash = filteredItemHash.itemHash();
                    // TODO work out which tree filtered item belongs to
                    stateChangeItemsHasher.addLeaf(hash.toByteArray());
                }
                case TRACE_DATA -> traceItemsHasher.addLeaf(itemHash);
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
