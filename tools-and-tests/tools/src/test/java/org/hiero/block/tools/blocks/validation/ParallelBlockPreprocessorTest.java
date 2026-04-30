// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.validation;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileItem;
import com.hedera.hapi.block.stream.output.BlockFooter;
import com.hedera.hapi.block.stream.output.BlockHeader;
import com.hedera.hapi.node.base.Timestamp;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.validation.ParallelBlockPreprocessor.PreprocessedData;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Tests for {@link ParallelBlockPreprocessor}. */
class ParallelBlockPreprocessorTest {

    private static final BlockItem HEADER_ITEM = BlockItem.newBuilder()
            .blockHeader(BlockHeader.newBuilder()
                    .number(0)
                    .blockTimestamp(Timestamp.newBuilder()
                            .seconds(1700000000L)
                            .nanos(123456789)
                            .build())
                    .build())
            .build();
    private static final BlockItem RECORD_FILE_ITEM =
            BlockItem.newBuilder().recordFile(RecordFileItem.DEFAULT).build();
    private static final BlockItem FOOTER_ITEM = BlockItem.newBuilder()
            .blockFooter(BlockFooter.newBuilder()
                    .previousBlockRootHash(Bytes.EMPTY)
                    .rootHashOfAllBlockHashesTree(Bytes.EMPTY)
                    .startOfBlockStateRootHash(Bytes.EMPTY)
                    .build())
            .build();
    private static final BlockItem PROOF_ITEM =
            BlockItem.newBuilder().blockProof(BlockProof.DEFAULT).build();

    private static final BlockUnparsed VALID_BLOCK =
            toUnparsed(new Block(List.of(HEADER_ITEM, RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM)));

    /** Block with no header — instant extraction should return null. */
    private static final BlockUnparsed NO_HEADER_BLOCK =
            toUnparsed(new Block(List.of(RECORD_FILE_ITEM, FOOTER_ITEM, PROOF_ITEM)));

    /** Block with no record file — recordFileBytes extraction should return null. */
    private static final BlockUnparsed NO_RECORD_FILE_BLOCK =
            toUnparsed(new Block(List.of(HEADER_ITEM, FOOTER_ITEM, PROOF_ITEM)));

    private static BlockUnparsed toUnparsed(Block block) {
        try {
            return BlockUnparsed.PROTOBUF.parse(Block.PROTOBUF.toBytes(block));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse block", e);
        }
    }

    @Nested
    @DisplayName("Hash computation")
    class HashTests {
        @Test
        @DisplayName("Pre-computed hash matches direct hashBlock() call")
        void hashMatchesDirectCall() {
            PreprocessedData ppd = ParallelBlockPreprocessor.preprocess(VALID_BLOCK);
            byte[] directHash = hashBlock(VALID_BLOCK);
            assertNotNull(ppd.blockHash());
            assertArrayEquals(directHash, ppd.blockHash());
        }
    }

    @Nested
    @DisplayName("Block instant extraction")
    class InstantTests {
        @Test
        @DisplayName("Extracts correct instant from BlockHeader")
        void extractsCorrectInstant() {
            PreprocessedData ppd = ParallelBlockPreprocessor.preprocess(VALID_BLOCK);
            assertNotNull(ppd.blockInstant());
            assertEquals(Instant.ofEpochSecond(1700000000L, 123456789), ppd.blockInstant());
        }

        @Test
        @DisplayName("Returns null when no BlockHeader present")
        void returnsNullForMissingHeader() {
            PreprocessedData ppd = ParallelBlockPreprocessor.preprocess(NO_HEADER_BLOCK);
            assertNull(ppd.blockInstant());
        }
    }

    @Nested
    @DisplayName("RecordFile bytes extraction")
    class RecordFileBytesTests {
        @Test
        @DisplayName("Extracts non-null RecordFile bytes when present")
        void extractsRecordFileBytes() {
            PreprocessedData ppd = ParallelBlockPreprocessor.preprocess(VALID_BLOCK);
            assertNotNull(ppd.recordFileBytes());
        }

        @Test
        @DisplayName("Returns null when no RecordFile item present")
        void returnsNullForMissingRecordFile() {
            PreprocessedData ppd = ParallelBlockPreprocessor.preprocess(NO_RECORD_FILE_BLOCK);
            assertNull(ppd.recordFileBytes());
        }
    }

    @Nested
    @DisplayName("Thread safety")
    class ThreadSafetyTests {
        @Test
        @DisplayName("Parallel invocations produce consistent results")
        void parallelInvocationsProduceConsistentResults() throws Exception {
            int threadCount = 8;
            ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            List<Future<PreprocessedData>> futures = new ArrayList<>();
            for (int i = 0; i < threadCount; i++) {
                futures.add(pool.submit(() -> ParallelBlockPreprocessor.preprocess(VALID_BLOCK)));
            }
            byte[] expectedHash = hashBlock(VALID_BLOCK);
            Instant expectedInstant = Instant.ofEpochSecond(1700000000L, 123456789);
            for (Future<PreprocessedData> f : futures) {
                PreprocessedData ppd = f.get();
                assertArrayEquals(expectedHash, ppd.blockHash());
                assertEquals(expectedInstant, ppd.blockInstant());
                assertNotNull(ppd.recordFileBytes());
            }
            pool.shutdown();
        }
    }
}
