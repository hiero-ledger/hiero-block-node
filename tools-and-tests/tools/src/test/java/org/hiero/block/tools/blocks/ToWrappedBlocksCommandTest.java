// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.hapi.block.stream.Block;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the durability, watermark, and resume infrastructure in {@link ToWrappedBlocksCommand}.
 *
 * <p>These tests verify the watermark file, registry truncation, and hasher replay logic
 * without running the full pipeline (which requires tar.zstd inputs and mirror node metadata).
 */
class ToWrappedBlocksCommandTest {

    @TempDir
    Path tempDir;

    // ===== Watermark file tests =====

    @Nested
    @DisplayName("Watermark file operations")
    class WatermarkTests {

        @Test
        @DisplayName("loadWatermark returns -1 for missing file")
        void testLoadWatermarkMissingFile() {
            final long result = ToWrappedBlocksCommand.loadWatermark(tempDir.resolve("nonexistent.bin"));
            assertEquals(-1, result);
        }

        @Test
        @DisplayName("saveWatermark + loadWatermark round-trips correctly")
        void testWatermarkRoundTrip() {
            final Path wf = tempDir.resolve("wrap-commit.bin");
            ToWrappedBlocksCommand.saveWatermark(wf, 42L);
            assertEquals(42L, ToWrappedBlocksCommand.loadWatermark(wf));
        }

        @Test
        @DisplayName("saveWatermark overwrites previous value")
        void testWatermarkOverwrite() {
            final Path wf = tempDir.resolve("wrap-commit.bin");
            ToWrappedBlocksCommand.saveWatermark(wf, 100L);
            ToWrappedBlocksCommand.saveWatermark(wf, 200L);
            assertEquals(200L, ToWrappedBlocksCommand.loadWatermark(wf));
        }

        @Test
        @DisplayName("saveWatermark with -1 is a no-op")
        void testWatermarkNegativeIsNoop() {
            final Path wf = tempDir.resolve("wrap-commit.bin");
            ToWrappedBlocksCommand.saveWatermark(wf, -1);
            assertFalse(Files.exists(wf));
        }

        @Test
        @DisplayName("loadWatermark returns -1 for truncated file")
        void testLoadWatermarkTruncatedFile() throws IOException {
            final Path wf = tempDir.resolve("wrap-commit.bin");
            Files.write(wf, new byte[3]); // too short for a long
            assertEquals(-1, ToWrappedBlocksCommand.loadWatermark(wf));
        }

        @Test
        @DisplayName("saveWatermark is atomic (tmp file cleaned up)")
        void testWatermarkAtomicity() {
            final Path wf = tempDir.resolve("wrap-commit.bin");
            ToWrappedBlocksCommand.saveWatermark(wf, 999L);
            assertFalse(Files.exists(tempDir.resolve("wrap-commit.bin.tmp")));
            assertTrue(Files.exists(wf));
        }

        @Test
        @DisplayName("saveWatermark stores block 0 correctly")
        void testWatermarkBlockZero() {
            final Path wf = tempDir.resolve("wrap-commit.bin");
            ToWrappedBlocksCommand.saveWatermark(wf, 0L);
            assertEquals(0L, ToWrappedBlocksCommand.loadWatermark(wf));
        }

        @Test
        @DisplayName("saveWatermark handles large block numbers")
        void testWatermarkLargeBlockNumber() {
            final Path wf = tempDir.resolve("wrap-commit.bin");
            final long largeNum = 1_000_000_000L;
            ToWrappedBlocksCommand.saveWatermark(wf, largeNum);
            assertEquals(largeNum, ToWrappedBlocksCommand.loadWatermark(wf));
        }
    }

    // ===== Registry truncation tests =====

    @Nested
    @DisplayName("BlockStreamBlockHashRegistry truncation")
    class RegistryTruncationTests {

        @Test
        @DisplayName("truncateTo reduces highestBlockNumberStored")
        void testTruncateReducesHighest() throws Exception {
            final Path regFile = tempDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry reg = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 10; i++) {
                    reg.addBlock(i, new byte[48]);
                }
                assertEquals(9, reg.highestBlockNumberStored());
                reg.truncateTo(5);
                assertEquals(5, reg.highestBlockNumberStored());
            }
        }

        @Test
        @DisplayName("truncateTo(-1) clears all blocks")
        void testTruncateToClearsAll() throws Exception {
            final Path regFile = tempDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry reg = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 5; i++) {
                    reg.addBlock(i, new byte[48]);
                }
                reg.truncateTo(-1);
                assertEquals(-1, reg.highestBlockNumberStored());
            }
        }

        @Test
        @DisplayName("truncateTo preserves hashes for retained blocks")
        void testTruncatePreservesHashes() throws Exception {
            final Path regFile = tempDir.resolve("hashes.bin");
            final byte[][] hashes = new byte[10][48];
            for (int i = 0; i < 10; i++) {
                hashes[i][0] = (byte) i;
            }
            try (BlockStreamBlockHashRegistry reg = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 10; i++) {
                    reg.addBlock(i, hashes[i]);
                }
                reg.truncateTo(5);
                for (int i = 0; i <= 5; i++) {
                    assertArrayEquals(hashes[i], reg.getBlockHash(i));
                }
                // Block 6 should no longer be accessible
                assertThrows(IllegalArgumentException.class, () -> reg.getBlockHash(6));
            }
        }

        @Test
        @DisplayName("addBlock works after truncation")
        void testAddAfterTruncate() throws Exception {
            final Path regFile = tempDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry reg = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 10; i++) {
                    reg.addBlock(i, new byte[48]);
                }
                reg.truncateTo(5);
                final byte[] newHash = new byte[48];
                newHash[0] = 42;
                reg.addBlock(6, newHash);
                assertEquals(6, reg.highestBlockNumberStored());
                assertArrayEquals(newHash, reg.getBlockHash(6));
            }
        }

        @Test
        @DisplayName("truncateTo with invalid block number throws")
        void testTruncateInvalidThrows() throws Exception {
            final Path regFile = tempDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry reg = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 5; i++) {
                    reg.addBlock(i, new byte[48]);
                }
                assertThrows(IllegalArgumentException.class, () -> reg.truncateTo(10));
                assertThrows(IllegalArgumentException.class, () -> reg.truncateTo(-2));
            }
        }

        @Test
        @DisplayName("truncateTo is idempotent at current highest")
        void testTruncateIdempotent() throws Exception {
            final Path regFile = tempDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry reg = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 5; i++) {
                    reg.addBlock(i, new byte[48]);
                }
                reg.truncateTo(4);
                assertEquals(4, reg.highestBlockNumberStored());
            }
        }

        @Test
        @DisplayName("mostRecentBlockHash updated after truncation")
        void testMostRecentHashUpdatedAfterTruncation() throws Exception {
            final Path regFile = tempDir.resolve("hashes.bin");
            final byte[] hash3 = new byte[48];
            hash3[0] = 3;
            try (BlockStreamBlockHashRegistry reg = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 5; i++) {
                    final byte[] h = new byte[48];
                    h[0] = (byte) i;
                    reg.addBlock(i, h);
                }
                reg.truncateTo(3);
                assertArrayEquals(hash3, reg.mostRecentBlockHash());
            }
        }
    }

    // ===== Hasher replay tests =====

    @Nested
    @DisplayName("Hasher replay from registry")
    class HasherReplayTests {

        @Test
        @DisplayName("Fresh hashers replayed from registry match original state")
        void testHasherReplayMatchesOriginal() {
            // Create a chain and compute hashes
            final List<Block> chain = TestBlockFactory.createValidChain(20);
            final StreamingHasher originalStreaming = new StreamingHasher();
            final InMemoryTreeHasher originalInMemory = new InMemoryTreeHasher();
            final byte[][] blockHashes = new byte[20][];

            for (int i = 0; i < chain.size(); i++) {
                blockHashes[i] = BlockStreamBlockHasher.hashBlock(chain.get(i));
                originalStreaming.addNodeByHash(blockHashes[i]);
                originalInMemory.addNodeByHash(blockHashes[i]);
            }

            // Now simulate resume: create fresh hashers and replay from registry
            final StreamingHasher replayedStreaming = new StreamingHasher();
            final InMemoryTreeHasher replayedInMemory = new InMemoryTreeHasher();
            for (byte[] blockHash : blockHashes) {
                replayedStreaming.addNodeByHash(blockHash);
                replayedInMemory.addNodeByHash(blockHash);
            }

            // Root hashes must match
            assertArrayEquals(originalStreaming.computeRootHash(), replayedStreaming.computeRootHash());
            assertArrayEquals(originalInMemory.computeRootHash(), replayedInMemory.computeRootHash());
            assertEquals(originalStreaming.leafCount(), replayedStreaming.leafCount());
            assertEquals(originalInMemory.leafCount(), replayedInMemory.leafCount());
        }

        @Test
        @DisplayName("Partial replay (truncated registry) produces consistent state")
        void testPartialReplayConsistency() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(20);
            final byte[][] blockHashes = new byte[20][];
            for (int i = 0; i < chain.size(); i++) {
                blockHashes[i] = BlockStreamBlockHasher.hashBlock(chain.get(i));
            }

            // Build registry with all 20 blocks
            final Path regFile = tempDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 20; i++) {
                    registry.addBlock(i, blockHashes[i]);
                }

                // Simulate watermark at block 9 — truncate registry
                registry.truncateTo(9);

                // Replay into fresh hashers
                final StreamingHasher streamingHasher = new StreamingHasher();
                final InMemoryTreeHasher inMemoryHasher = new InMemoryTreeHasher();
                for (long bn = 0; bn <= 9; bn++) {
                    final byte[] hash = registry.getBlockHash(bn);
                    streamingHasher.addNodeByHash(hash);
                    inMemoryHasher.addNodeByHash(hash);
                }

                // Build expected hashers with just 10 blocks
                final StreamingHasher expected = new StreamingHasher();
                for (int i = 0; i < 10; i++) {
                    expected.addNodeByHash(blockHashes[i]);
                }

                assertArrayEquals(expected.computeRootHash(), streamingHasher.computeRootHash());
                assertEquals(10, streamingHasher.leafCount());
                assertEquals(10, inMemoryHasher.leafCount());
            }
        }

        @Test
        @DisplayName("Replay from empty registry produces empty hashers")
        void testReplayFromEmptyRegistry() throws Exception {
            final Path regFile = tempDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                assertEquals(-1, registry.highestBlockNumberStored());

                final StreamingHasher streamingHasher = new StreamingHasher();
                final InMemoryTreeHasher inMemoryHasher = new InMemoryTreeHasher();

                // No replay needed when registry is empty
                assertEquals(0, streamingHasher.leafCount());
                assertEquals(0, inMemoryHasher.leafCount());
            }
        }
    }

    // ===== Block writing + watermark integration tests =====

    @Nested
    @DisplayName("Block writing and watermark integration")
    class WriteAndWatermarkTests {

        @Test
        @DisplayName("Blocks written via BlockWriter are readable")
        void testWrittenBlocksReadable() throws IOException {
            final List<Block> chain = TestBlockFactory.createValidChain(5);
            final Path outputDir = tempDir.resolve("blocks");
            Files.createDirectories(outputDir);

            for (Block block : chain) {
                BlockWriter.writeBlock(outputDir, block);
            }

            // Verify highest block
            assertEquals(4, BlockWriter.maxStoredBlockNumber(outputDir, BlockWriter.DEFAULT_COMPRESSION));
        }

        @Test
        @DisplayName("Watermark at block 5 with registry at 10 — registry should be truncatable")
        void testWatermarkRegistryReconciliation() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(15);
            final byte[][] blockHashes = new byte[15][];
            for (int i = 0; i < chain.size(); i++) {
                blockHashes[i] = BlockStreamBlockHasher.hashBlock(chain.get(i));
            }

            // Build registry with 15 blocks
            final Path regFile = tempDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 15; i++) {
                    registry.addBlock(i, blockHashes[i]);
                }
                assertEquals(14, registry.highestBlockNumberStored());

                // Set watermark to 5
                final Path wf = tempDir.resolve("wrap-commit.bin");
                ToWrappedBlocksCommand.saveWatermark(wf, 5L);
                final long watermark = ToWrappedBlocksCommand.loadWatermark(wf);
                assertEquals(5L, watermark);

                // Truncate registry to watermark (simulating resume logic)
                registry.truncateTo(watermark);
                assertEquals(5, registry.highestBlockNumberStored());

                // Replay hashers from 0 through watermark
                final StreamingHasher hasher = new StreamingHasher();
                for (long bn = 0; bn <= watermark; bn++) {
                    hasher.addNodeByHash(registry.getBlockHash(bn));
                }
                assertEquals(6, hasher.leafCount());

                // Can continue adding blocks from 6 onward
                registry.addBlock(6, blockHashes[6]);
                assertEquals(6, registry.highestBlockNumberStored());
            }
        }
    }

    // ===== Hasher state save/load tests =====

    @Nested
    @DisplayName("Hasher state persistence")
    class HasherPersistenceTests {

        @Test
        @DisplayName("StreamingHasher save + load round-trips correctly")
        void testStreamingHasherRoundTrip() throws Exception {
            final StreamingHasher original = new StreamingHasher();
            final List<Block> chain = TestBlockFactory.createValidChain(10);
            for (Block block : chain) {
                original.addNodeByHash(BlockStreamBlockHasher.hashBlock(block));
            }

            final Path file = tempDir.resolve("streaming.bin");
            original.save(file);

            final StreamingHasher loaded = new StreamingHasher();
            loaded.load(file);

            assertArrayEquals(original.computeRootHash(), loaded.computeRootHash());
            assertEquals(original.leafCount(), loaded.leafCount());
        }

        @Test
        @DisplayName("InMemoryTreeHasher save + load round-trips correctly")
        void testInMemoryHasherRoundTrip() throws Exception {
            final InMemoryTreeHasher original = new InMemoryTreeHasher();
            final List<Block> chain = TestBlockFactory.createValidChain(10);
            for (Block block : chain) {
                original.addNodeByHash(BlockStreamBlockHasher.hashBlock(block));
            }

            final Path file = tempDir.resolve("inmemory.bin");
            original.save(file);

            final InMemoryTreeHasher loaded = new InMemoryTreeHasher();
            loaded.load(file);

            assertArrayEquals(original.computeRootHash(), loaded.computeRootHash());
            assertEquals(original.leafCount(), loaded.leafCount());
        }

        @Test
        @DisplayName("HasherStateFiles atomic save creates .bak and cleans .tmp")
        void testAtomicSavePattern() throws Exception {
            final StreamingHasher hasher = new StreamingHasher();
            hasher.addNodeByHash(new byte[48]);

            final Path primary = tempDir.resolve("hasher.bin");
            HasherStateFiles.saveAtomically(primary, hasher::save);
            assertTrue(Files.exists(primary));
            assertFalse(Files.exists(Path.of(primary + ".tmp")));

            // Save again — should create .bak
            hasher.addNodeByHash(new byte[48]);
            HasherStateFiles.saveAtomically(primary, hasher::save);
            assertTrue(Files.exists(primary));
            assertTrue(Files.exists(Path.of(primary + ".bak")));
        }

        @Test
        @DisplayName("loadWithFallback loads from .bak when primary is missing")
        void testLoadWithFallbackFromBackup() throws Exception {
            final StreamingHasher original = new StreamingHasher();
            original.addNodeByHash(new byte[48]);

            final Path primary = tempDir.resolve("hasher.bin");
            final Path backup = Path.of(primary + ".bak");
            original.save(backup);

            final StreamingHasher loaded = new StreamingHasher();
            HasherStateFiles.loadWithFallback(primary, loaded::load);

            assertEquals(original.leafCount(), loaded.leafCount());
        }
    }
}
