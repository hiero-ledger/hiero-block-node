// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification;

import static org.hiero.block.common.hasher.HashingUtilities.getBlockItemHash;
import static org.hiero.block.common.hasher.HashingUtilities.noThrowSha384HashOf;

import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import org.hiero.block.common.hasher.HashingUtilities;
import org.hiero.block.common.hasher.NaiveStreamingTreeHasher;
import org.hiero.block.internal.BlockItemUnparsed;

/**
 * Factory for creating synthetic blocks that pass {@code ExtendedMerkleTreeSession} verification.
 *
 * <p>Blocks use the hash-of-hash signature path: the proof signature is {@code SHA384(blockRootHash)}.
 * HAPI version 1.2.3 routes to {@code ExtendedMerkleTreeSession} (>= 0.72.0).
 */
final class VerifiableBlockFactory {

    private static final Timestamp BLOCK_TIMESTAMP = new Timestamp(123L, 456);
    private static final SemanticVersion HAPI_VERSION = new SemanticVersion(1, 2, 3, "a", "b");
    private static final SemanticVersion SOFTWARE_VERSION = new SemanticVersion(4, 5, 6, "c", "d");
    private static final Bytes ZERO_HASH = Bytes.wrap(new byte[HashingUtilities.HASH_SIZE]);

    private VerifiableBlockFactory() {}

    /**
     * Creates unparsed block items for a verifiable synthetic block.
     *
     * @param blockNumber the block number
     * @param previousBlockHash the hash of the previous block, or null for genesis
     * @return list of unparsed block items (header, round header, footer, proof)
     */
    static List<BlockItemUnparsed> createBlockItems(long blockNumber, @Nullable Bytes previousBlockHash) {
        Bytes previousHash = previousBlockHash != null ? previousBlockHash : ZERO_HASH;

        BlockItem header = new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_HEADER,
                new BlockHeader(
                        HAPI_VERSION, SOFTWARE_VERSION, blockNumber, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384)));
        BlockItem roundHeader =
                new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, new RoundHeader(blockNumber * 10L)));
        BlockItem footer = new BlockItem(
                new OneOf<>(ItemOneOfType.BLOCK_FOOTER, new BlockFooter(previousHash, ZERO_HASH, Bytes.EMPTY)));

        Bytes blockHash = computeBlockHash(blockNumber, previousBlockHash);
        Bytes signature = noThrowSha384HashOf(blockHash);
        BlockItem proof = new BlockItem(new OneOf<>(
                ItemOneOfType.BLOCK_PROOF,
                BlockProof.newBuilder()
                        .block(blockNumber)
                        .signedBlockProof(TssSignedBlockProof.newBuilder()
                                .blockSignature(signature)
                                .build())
                        .build()));

        return List.of(header, roundHeader, footer, proof).stream()
                .map(VerifiableBlockFactory::toUnparsed)
                .toList();
    }

    /**
     * Computes the block root hash for a synthetic block. Use the returned hash as
     * the {@code previousBlockHash} argument when creating the next block in the chain.
     *
     * @param blockNumber the block number
     * @param previousBlockHash the hash of the previous block, or null for genesis
     * @return the computed block root hash
     */
    static Bytes computeBlockHash(long blockNumber, @Nullable Bytes previousBlockHash) {
        Bytes previousHash = previousBlockHash != null ? previousBlockHash : ZERO_HASH;

        BlockItemUnparsed headerUnparsed = BlockItemUnparsed.newBuilder()
                .blockHeader(BlockHeader.PROTOBUF.toBytes(new BlockHeader(
                        HAPI_VERSION, SOFTWARE_VERSION, blockNumber, BLOCK_TIMESTAMP, BlockHashAlgorithm.SHA2_384)))
                .build();
        BlockItemUnparsed roundHeaderUnparsed = BlockItemUnparsed.newBuilder()
                .roundHeader(RoundHeader.PROTOBUF.toBytes(new RoundHeader(blockNumber * 10L)))
                .build();

        NaiveStreamingTreeHasher outputHasher = new NaiveStreamingTreeHasher();
        outputHasher.addLeaf(getBlockItemHash(headerUnparsed));
        NaiveStreamingTreeHasher consensusHasher = new NaiveStreamingTreeHasher();
        consensusHasher.addLeaf(getBlockItemHash(roundHeaderUnparsed));

        return HashingUtilities.computeFinalBlockHash(
                BLOCK_TIMESTAMP,
                previousHash,
                ZERO_HASH,
                Bytes.EMPTY,
                new NaiveStreamingTreeHasher(),
                outputHasher,
                consensusHasher,
                new NaiveStreamingTreeHasher(),
                new NaiveStreamingTreeHasher());
    }

    private static BlockItemUnparsed toUnparsed(BlockItem item) {
        try {
            return BlockItemUnparsed.PROTOBUF.parse(BlockItem.PROTOBUF.toBytes(item));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
