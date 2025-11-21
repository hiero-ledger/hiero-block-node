// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.historicalblocks;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.github.luben.zstd.Zstd;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockItem.ItemOneOfType;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.ChainOfTrustProof;
import com.hedera.hapi.block.stream.MerkleSiblingHash;
import com.hedera.hapi.block.stream.TssSignedBlockProof;
import com.hedera.hapi.block.stream.input.RoundHeader;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.BlockHashAlgorithm;
import com.hedera.hapi.node.base.SemanticVersion;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.OneOf;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.util.List;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor.Format;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit Test for BlockAccessor interface and its default methods.
 */
public class BlockAccessorTest {
    private static final Block SAMPLE_BLOCK = new Block(List.of(
            new BlockItem(new OneOf<>(
                    ItemOneOfType.BLOCK_HEADER,
                    new BlockHeader(
                            new SemanticVersion(1, 2, 3, "a", "b"),
                            new SemanticVersion(4, 5, 6, "c", "d"),
                            123,
                            new Timestamp(123L, 456),
                            BlockHashAlgorithm.SHA2_384))),
            new BlockItem(new OneOf<>(ItemOneOfType.ROUND_HEADER, new RoundHeader(827))),
            new BlockItem(new OneOf<>(
                    ItemOneOfType.BLOCK_PROOF,
                    BlockProof.newBuilder()
                            .block(0)
                            .blockSignature(Bytes.wrap("block_signature".getBytes()))
                            .previousBlockRootHash(Bytes.wrap("previousBlockRootHash".getBytes()))
                            .siblingHashes(MerkleSiblingHash.newBuilder()
                                    .siblingHash(Bytes.wrap("sibling_hash".getBytes()))
                                    .build())
                            .signedBlockProof(TssSignedBlockProof.newBuilder()
                                    .blockSignature(Bytes.wrap("hints_signature"))
                                    .build())
                            .startOfBlockStateRootHash(Bytes.wrap("startOfBlockStateRootHash".getBytes()))
                            .verificationKey(Bytes.wrap("verification_key".getBytes()))
                            .verificationKeyProof(ChainOfTrustProof.newBuilder()
                                    .wrapsProof(Bytes.wrap("verificationKeyProof".getBytes())))
                            .build()))));
    private static final Bytes SAMPLE_BLOCK_PROTOBUF_BYTES = Block.PROTOBUF.toBytes(SAMPLE_BLOCK);
    private static final Bytes SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES =
            Bytes.wrap(Zstd.compress(Block.PROTOBUF.toBytes(SAMPLE_BLOCK).toByteArray()));
    private static final Bytes SAMPLE_BLOCK_JSON_BYTES = Block.JSON.toBytes(SAMPLE_BLOCK);

    private static class TestBlockAccessor implements BlockAccessor {
        @Override
        public long blockNumber() {
            return 0;
        }

        @Override
        public Bytes blockBytes(final Format format) {
            return switch (format) {
                case JSON -> SAMPLE_BLOCK_JSON_BYTES;
                case PROTOBUF -> SAMPLE_BLOCK_PROTOBUF_BYTES;
                case ZSTD_PROTOBUF -> SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES;
            };
        }
    }

    @Test
    @DisplayName("Test blockUnparsed method")
    void testBlockUnparsed() throws ParseException {
        BlockAccessor accessor = new TestBlockAccessor();
        BlockUnparsed blockUnparsed = BlockUnparsed.PROTOBUF.parse(SAMPLE_BLOCK_PROTOBUF_BYTES);
        assertEquals(blockUnparsed, accessor.blockUnparsed());
        // create a parsing failure
        BlockAccessor emptyAccessor = new ParseFailureBlockAccessor();
        assertDoesNotThrow(emptyAccessor::blockUnparsed);
        assertNull(emptyAccessor.blockUnparsed());
    }

    @Test
    @DisplayName("Test blockBytes method with supported format")
    void testBlockBytesSupportedFormats() {
        BlockAccessor accessor = new TestBlockAccessor();
        assertEquals(SAMPLE_BLOCK_PROTOBUF_BYTES, accessor.blockBytes(Format.PROTOBUF));
        assertEquals(SAMPLE_BLOCK_ZSTD_PROTOBUF_BYTES, accessor.blockBytes(Format.ZSTD_PROTOBUF));
        assertEquals(SAMPLE_BLOCK_JSON_BYTES, accessor.blockBytes(Format.JSON));
    }

    private static class ParseFailureBlockAccessor implements BlockAccessor {
        @Override
        public long blockNumber() {
            return 0;
        }

        @Override
        public Bytes blockBytes(Format format) {
            return Bytes.wrap(new byte[] {1, 2, 3, 4, 5});
        }
    }
}
