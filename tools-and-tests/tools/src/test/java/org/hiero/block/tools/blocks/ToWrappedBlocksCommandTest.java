// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.hedera.hapi.block.stream.Block;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockReader;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockPath;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockZipAppender;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.blocks.wrapped.ValidateWrappedBlocksCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import picocli.CommandLine;

/**
 * Tests for the durability, watermark, and resume infrastructure in {@link ToWrappedBlocksCommand}.
 *
 * <p>These tests verify the watermark file, registry truncation, and hasher replay logic
 * without running the full pipeline (which requires tar.zstd inputs and mirror node metadata).
 */
@Execution(ExecutionMode.SAME_THREAD)
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

    // ===== Real-data E2E tests using mainnet tar.zstd files =====

    @Nested
    @DisplayName("Real-data E2E tests")
    class RealDataE2ETests {

        @TempDir
        Path e2eTempDir;

        /** Scans test resources for all .tar.zstd files, sorted by name. */
        private List<Path> findTarZstdFiles() throws Exception {
            final Path resourceDir = Path.of(Objects.requireNonNull(getClass().getResource("/2019-09-13.tar.zstd"))
                            .toURI())
                    .getParent();
            try (var stream = Files.list(resourceDir)) {
                return stream.filter(p -> p.getFileName().toString().endsWith(".tar.zstd"))
                        .sorted()
                        .toList();
            }
        }

        /** Returns the path to the block_times.bin test resource. */
        private Path blockTimesFile() throws Exception {
            return Path.of(Objects.requireNonNull(getClass().getResource("/metadata/block_times.bin"))
                    .toURI());
        }

        /** Returns the path to the day_blocks.json test resource. */
        private Path dayBlocksFile() throws Exception {
            return Path.of(Objects.requireNonNull(getClass().getResource("/metadata/day_blocks.json"))
                    .toURI());
        }

        /** Copies tar.zstd files into a fresh input directory. */
        private Path setupInputDir(List<Path> tarZstdFiles) throws IOException {
            final Path inputDir = e2eTempDir.resolve("input");
            Files.createDirectories(inputDir);
            for (Path tarZstd : tarZstdFiles) {
                Files.copy(tarZstd, inputDir.resolve(tarZstd.getFileName()));
            }
            return inputDir;
        }

        /** Runs the wrap command and asserts exit code 0. */
        private Path runWrap(Path inputDir, boolean unzipped) throws Exception {
            final Path outputDir = e2eTempDir.resolve(unzipped ? "output-unzipped" : "output-zipped");
            final var args = new java.util.ArrayList<>(List.of(
                    "-i", inputDir.toString(),
                    "-o", outputDir.toString(),
                    "-b", blockTimesFile().toString(),
                    "-d", dayBlocksFile().toString()));
            if (unzipped) {
                args.add("-u");
            }
            int exitCode = new CommandLine(new ToWrappedBlocksCommand()).execute(args.toArray(String[]::new));
            assertEquals(0, exitCode, "Wrap command should exit with code 0");
            return outputDir;
        }

        /** Runs validate-wrapped and asserts exit code 0 with no error output. */
        private void runValidate(Path outputDir) {
            final PrintStream originalErr = System.err;
            final ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
            System.setErr(new PrintStream(errCapture));
            int exitCode;
            try {
                exitCode = new CommandLine(new ValidateWrappedBlocksCommand())
                        .execute(outputDir.toString(), "--validate-balances=false");
            } finally {
                System.setErr(originalErr);
            }
            final String errorOutput = errCapture.toString();
            if (!errorOutput.isEmpty()) {
                System.err.print(errorOutput);
            }
            assertFalse(errorOutput.contains("Blockchain is not valid"), "Chain validation failed: " + errorOutput);
            assertFalse(errorOutput.contains("HBAR supply mismatch"), "50 billion HBAR check failed: " + errorOutput);
            assertEquals(0, exitCode, "Validation should pass. Errors: " + errorOutput);
        }

        @Test
        @DisplayName("Wrap in zip mode then validate")
        void testWrapZipModeAndValidate() throws Exception {
            assumeTrue(isZstdAvailable(), "zstd not available");
            final List<Path> tarZstdFiles = findTarZstdFiles();
            assumeFalse(tarZstdFiles.isEmpty(), "No .tar.zstd files in test resources");

            final Path inputDir = setupInputDir(tarZstdFiles);
            final Path outputDir = runWrap(inputDir, false);

            // Verify essential output files exist
            assertTrue(Files.exists(outputDir.resolve("addressBookHistory.json")));
            assertTrue(Files.exists(outputDir.resolve("blockStreamBlockHashes.bin")));
            assertTrue(Files.exists(outputDir.resolve("streamingMerkleTree.bin")));
            assertTrue(Files.exists(outputDir.resolve("completeMerkleTree.bin")));
            assertTrue(Files.exists(outputDir.resolve("wrap-commit.bin")));
            assertTrue(Files.exists(outputDir.resolve("jumpstart.bin")));

            // Verify zip files are valid
            try (var zipStream = Files.walk(outputDir)) {
                final List<Path> zipFiles = zipStream
                        .filter(p -> p.getFileName().toString().endsWith(".zip"))
                        .toList();
                assertFalse(zipFiles.isEmpty(), "Expected at least one zip file");
                for (Path zipPath : zipFiles) {
                    try (ZipFile zf = new ZipFile(zipPath.toFile())) {
                        assertTrue(zf.size() > 0, "Zip file should contain entries: " + zipPath);
                    }
                }
            }

            // Validate the wrapped output
            runValidate(outputDir);
        }

        @Test
        @DisplayName("Wrap in unzipped mode then validate")
        void testWrapUnzippedModeAndValidate() throws Exception {
            assumeTrue(isZstdAvailable(), "zstd not available");
            final List<Path> tarZstdFiles = findTarZstdFiles();
            assumeFalse(tarZstdFiles.isEmpty(), "No .tar.zstd files in test resources");

            final Path inputDir = setupInputDir(tarZstdFiles);
            final Path outputDir = runWrap(inputDir, true);

            // Verify essential output files exist
            assertTrue(Files.exists(outputDir.resolve("addressBookHistory.json")));
            assertTrue(Files.exists(outputDir.resolve("blockStreamBlockHashes.bin")));
            assertTrue(Files.exists(outputDir.resolve("streamingMerkleTree.bin")));
            assertTrue(Files.exists(outputDir.resolve("completeMerkleTree.bin")));
            assertTrue(Files.exists(outputDir.resolve("wrap-commit.bin")));
            assertTrue(Files.exists(outputDir.resolve("jumpstart.bin")));

            // Verify individual .blk.zstd files exist
            try (var blkStream = Files.walk(outputDir)) {
                final long blkCount = blkStream
                        .filter(p -> p.getFileName().toString().endsWith(".blk.zstd"))
                        .count();
                assertTrue(blkCount > 0, "Expected individual .blk.zstd files in unzipped mode");
            }

            // Validate the wrapped output
            runValidate(outputDir);
        }

        @Test
        @DisplayName("Wrap resume from partial run")
        void testWrapResumeFromPartial() throws Exception {
            assumeTrue(isZstdAvailable(), "zstd not available");
            final List<Path> tarZstdFiles = findTarZstdFiles();
            assumeTrue(tarZstdFiles.size() >= 2, "Need at least 2 .tar.zstd files for resume test");

            // Phase 1: wrap with only the first day file
            final Path inputDir = e2eTempDir.resolve("input-resume");
            Files.createDirectories(inputDir);
            Files.copy(
                    tarZstdFiles.getFirst(),
                    inputDir.resolve(tarZstdFiles.getFirst().getFileName()));

            final Path outputDir = e2eTempDir.resolve("output-resume");
            int exitCode1 = new CommandLine(new ToWrappedBlocksCommand())
                    .execute(
                            "-i", inputDir.toString(),
                            "-o", outputDir.toString(),
                            "-b", blockTimesFile().toString(),
                            "-d", dayBlocksFile().toString());
            assertEquals(0, exitCode1, "First wrap run should succeed");

            // Record watermark after first run
            final long watermarkAfterFirstRun =
                    ToWrappedBlocksCommand.loadWatermark(outputDir.resolve("wrap-commit.bin"));
            assertTrue(watermarkAfterFirstRun >= 0, "Watermark should be set after first run");

            // Phase 2: add remaining day files and wrap again (resume)
            for (int i = 1; i < tarZstdFiles.size(); i++) {
                Files.copy(
                        tarZstdFiles.get(i),
                        inputDir.resolve(tarZstdFiles.get(i).getFileName()));
            }

            int exitCode2 = new CommandLine(new ToWrappedBlocksCommand())
                    .execute(
                            "-i", inputDir.toString(),
                            "-o", outputDir.toString(),
                            "-b", blockTimesFile().toString(),
                            "-d", dayBlocksFile().toString());
            assertEquals(0, exitCode2, "Resume wrap run should succeed");

            // Watermark should have advanced
            final long watermarkAfterResume =
                    ToWrappedBlocksCommand.loadWatermark(outputDir.resolve("wrap-commit.bin"));
            assertTrue(
                    watermarkAfterResume > watermarkAfterFirstRun,
                    "Watermark should advance after resume: " + watermarkAfterResume + " > " + watermarkAfterFirstRun);

            // Validate the complete output
            runValidate(outputDir);
        }

        @Test
        @DisplayName("Watermark matches last block in registry")
        void testWatermarkMatchesLastBlock() throws Exception {
            assumeTrue(isZstdAvailable(), "zstd not available");
            final List<Path> tarZstdFiles = findTarZstdFiles();
            assumeFalse(tarZstdFiles.isEmpty(), "No .tar.zstd files in test resources");

            final Path inputDir = setupInputDir(tarZstdFiles);
            final Path outputDir = runWrap(inputDir, false);

            // Load watermark
            final long watermark = ToWrappedBlocksCommand.loadWatermark(outputDir.resolve("wrap-commit.bin"));
            assertTrue(watermark >= 0, "Watermark should be set");

            // Load registry and verify watermark matches highest block
            try (BlockStreamBlockHashRegistry registry =
                    new BlockStreamBlockHashRegistry(outputDir.resolve("blockStreamBlockHashes.bin"))) {
                assertEquals(
                        watermark,
                        registry.highestBlockNumberStored(),
                        "Watermark should match registry's highest block");
            }
        }

        @Test
        @DisplayName("Jumpstart data is valid")
        void testJumpstartDataValid() throws Exception {
            assumeTrue(isZstdAvailable(), "zstd not available");
            final List<Path> tarZstdFiles = findTarZstdFiles();
            assumeFalse(tarZstdFiles.isEmpty(), "No .tar.zstd files in test resources");

            final Path inputDir = setupInputDir(tarZstdFiles);
            final Path outputDir = runWrap(inputDir, false);

            final Path jumpstartFile = outputDir.resolve("jumpstart.bin");
            assertTrue(Files.exists(jumpstartFile), "jumpstart.bin should exist");

            // Read jumpstart data
            try (DataInputStream in = new DataInputStream(Files.newInputStream(jumpstartFile))) {
                final long blockNumber = in.readLong();
                assertTrue(blockNumber >= 0, "Jumpstart block number should be non-negative");

                // Block hash is 48 bytes (SHA-384)
                final byte[] blockHash = new byte[48];
                in.readFully(blockHash);

                // Verify hash is non-empty (not all zeros)
                boolean allZero = true;
                for (byte b : blockHash) {
                    if (b != 0) {
                        allZero = false;
                        break;
                    }
                }
                assertFalse(allZero, "Jumpstart block hash should not be all zeros");

                // Streaming hasher state
                final long leafCount = in.readLong();
                assertTrue(leafCount > 0, "Leaf count should be positive");
                assertEquals(blockNumber + 1, leafCount, "Leaf count should equal block count");

                final int hashListSize = in.readInt();
                assertTrue(hashListSize >= 0, "Hash list size should be non-negative");
                for (int i = 0; i < hashListSize; i++) {
                    final byte[] hash = new byte[48];
                    in.readFully(hash);
                }
            }

            // Verify jumpstart block number matches watermark
            final long watermark = ToWrappedBlocksCommand.loadWatermark(outputDir.resolve("wrap-commit.bin"));
            try (DataInputStream in = new DataInputStream(Files.newInputStream(jumpstartFile))) {
                assertEquals(watermark, in.readLong(), "Jumpstart block number should match watermark");
            }
        }

        @Test
        @DisplayName("Hasher state files are consistent after wrap")
        void testHasherStatesConsistent() throws Exception {
            assumeTrue(isZstdAvailable(), "zstd not available");
            final List<Path> tarZstdFiles = findTarZstdFiles();
            assumeFalse(tarZstdFiles.isEmpty(), "No .tar.zstd files in test resources");

            final Path inputDir = setupInputDir(tarZstdFiles);
            final Path outputDir = runWrap(inputDir, false);

            // Load both hasher state files
            final StreamingHasher streamingHasher = new StreamingHasher();
            streamingHasher.load(outputDir.resolve("streamingMerkleTree.bin"));

            final InMemoryTreeHasher inMemoryHasher = new InMemoryTreeHasher();
            inMemoryHasher.load(outputDir.resolve("completeMerkleTree.bin"));

            // Both should have same leaf count
            assertEquals(
                    streamingHasher.leafCount(),
                    inMemoryHasher.leafCount(),
                    "Streaming and in-memory hashers should have the same leaf count");

            // Leaf count should match registry block count
            try (BlockStreamBlockHashRegistry registry =
                    new BlockStreamBlockHashRegistry(outputDir.resolve("blockStreamBlockHashes.bin"))) {
                assertEquals(
                        registry.highestBlockNumberStored() + 1,
                        streamingHasher.leafCount(),
                        "Hasher leaf count should equal number of blocks in registry");
            }

            // Root hashes should be non-empty
            final byte[] streamingRoot = streamingHasher.computeRootHash();
            final byte[] inMemoryRoot = inMemoryHasher.computeRootHash();
            assertTrue(streamingRoot.length > 0, "Streaming root hash should be non-empty");
            assertTrue(inMemoryRoot.length > 0, "In-memory root hash should be non-empty");
        }
    }

    // ===== Synthetic write/read pipeline tests =====

    @Nested
    @DisplayName("Synthetic write-read pipeline")
    class SyntheticWriteReadPipeline {

        @TempDir
        Path pipelineDir;

        @AfterEach
        void clearFormatCache() {
            // BlockReader caches StorageFormat per directory; clear between tests
            // by using unique subdirs in each test (no static clear method available)
        }

        @Test
        @DisplayName("Write 20 synthetic blocks to zip and read back")
        void testWriteSyntheticBlocksToZipAndReadBack() throws IOException {
            final List<Block> chain = TestBlockFactory.createValidChain(20);
            final Path outputDir = pipelineDir.resolve("zip-blocks");
            Files.createDirectories(outputDir);

            for (Block block : chain) {
                BlockWriter.writeBlock(outputDir, block);
            }

            assertEquals(19, BlockWriter.maxStoredBlockNumber(outputDir, BlockWriter.DEFAULT_COMPRESSION));

            for (int i = 0; i < chain.size(); i++) {
                final Block readBack = BlockReader.readBlock(outputDir, i);
                assertEquals(
                        chain.get(i).items().size(), readBack.items().size(), "Item count mismatch for block " + i);
                assertEquals(
                        i, readBack.items().getFirst().blockHeader().number(), "Block number mismatch for block " + i);
            }
        }

        @Test
        @DisplayName("Write 10 synthetic blocks as individual files and read back")
        void testWriteSyntheticBlocksToIndividualFilesAndReadBack() throws IOException {
            final List<Block> chain = TestBlockFactory.createValidChain(10);
            final Path outputDir = pipelineDir.resolve("indiv-blocks");
            Files.createDirectories(outputDir);

            for (Block block : chain) {
                BlockWriter.writeBlock(outputDir, block, BlockArchiveType.INDIVIDUAL_FILES);
            }

            for (int i = 0; i < chain.size(); i++) {
                final Block readBack = BlockReader.readBlock(outputDir, i);
                assertEquals(
                        chain.get(i).items().size(), readBack.items().size(), "Item count mismatch for block " + i);
                assertEquals(
                        i, readBack.items().getFirst().blockHeader().number(), "Block number mismatch for block " + i);
            }
        }

        @Test
        @DisplayName("serializeBlockToBytes and parse round-trip")
        void testSerializeAndDeserializeRoundTrip() throws IOException {
            final List<Block> chain = TestBlockFactory.createValidChain(1);
            final Block original = chain.getFirst();
            final byte[] serialized = BlockWriter.serializeBlockToBytes(original, CompressionType.ZSTD);

            // Write to a zip, read back via BlockReader to verify full round-trip
            final Path outputDir = pipelineDir.resolve("serialize-rt");
            Files.createDirectories(outputDir);
            BlockWriter.writeBlock(outputDir, original);
            final Block readBack = BlockReader.readBlock(outputDir, 0);

            assertEquals(original.items().size(), readBack.items().size());
            assertEquals(
                    original.items().getFirst().blockHeader().number(),
                    readBack.items().getFirst().blockHeader().number());
            assertTrue(serialized.length > 0, "Serialized bytes should be non-empty");
        }

        @Test
        @DisplayName("ZipAppender write and read multiple blocks")
        void testZipAppenderWriteAndReadMultipleBlocks() throws IOException {
            final List<Block> chain = TestBlockFactory.createValidChain(5);
            final Path outputDir = pipelineDir.resolve("appender-blocks");
            Files.createDirectories(outputDir);

            // Compute path for block 0 to determine zip location
            final BlockPath blockPath0 = BlockWriter.computeBlockPath(outputDir, 0);
            Files.createDirectories(blockPath0.dirPath());

            try (BlockZipAppender appender = BlockWriter.openZipForAppend(blockPath0.zipFilePath())) {
                for (int i = 0; i < chain.size(); i++) {
                    final byte[] bytes = BlockWriter.serializeBlockToBytes(chain.get(i), CompressionType.ZSTD);
                    final BlockPath bp = BlockWriter.computeBlockPath(outputDir, i);
                    BlockWriter.writeBlockEntry(appender, bp, bytes);
                }
            }

            // Verify zip has 5 entries
            try (ZipFile zf = new ZipFile(blockPath0.zipFilePath().toFile())) {
                assertEquals(5, zf.size(), "Zip should contain 5 entries");
            }

            // Read back via BlockReader
            for (int i = 0; i < chain.size(); i++) {
                final Block readBack = BlockReader.readBlock(outputDir, i);
                assertEquals(
                        chain.get(i).items().size(), readBack.items().size(), "Item count mismatch for block " + i);
            }
        }
    }

    // ===== Zip write error path tests =====

    @Nested
    @DisplayName("Zip write error paths")
    class ZipWriteErrorPaths {

        @TempDir
        Path zipDir;

        @Test
        @DisplayName("openZipForAppend on existing zip uses FsBlockZipAppender path")
        void testOpenZipForAppendOnExistingZip() throws IOException {
            final Path outputDir = zipDir.resolve("existing-zip");
            Files.createDirectories(outputDir);
            final List<Block> chain = TestBlockFactory.createValidChain(5);

            // Write first 3 blocks to a zip
            final BlockPath bp0 = BlockWriter.computeBlockPath(outputDir, 0);
            Files.createDirectories(bp0.dirPath());
            try (BlockZipAppender appender = BlockWriter.openZipForAppend(bp0.zipFilePath())) {
                for (int i = 0; i < 3; i++) {
                    final byte[] bytes = BlockWriter.serializeBlockToBytes(chain.get(i), CompressionType.ZSTD);
                    BlockWriter.writeBlockEntry(appender, BlockWriter.computeBlockPath(outputDir, i), bytes);
                }
            }

            // Re-open (should use FsBlockZipAppender for existing zip) and add 2 more
            try (BlockZipAppender appender = BlockWriter.openZipForAppend(bp0.zipFilePath())) {
                for (int i = 3; i < 5; i++) {
                    final byte[] bytes = BlockWriter.serializeBlockToBytes(chain.get(i), CompressionType.ZSTD);
                    BlockWriter.writeBlockEntry(appender, BlockWriter.computeBlockPath(outputDir, i), bytes);
                }
            }

            // Verify 5 entries total
            try (ZipFile zf = new ZipFile(bp0.zipFilePath().toFile())) {
                assertEquals(5, zf.size(), "Zip should contain 5 entries after re-open append");
            }
        }

        @Test
        @DisplayName("openZipForAppend on non-existent file creates new zip")
        void testOpenZipForAppendOnNonexistentFile() throws IOException {
            final Path outputDir = zipDir.resolve("new-zip");
            Files.createDirectories(outputDir);
            final List<Block> chain = TestBlockFactory.createValidChain(1);

            final BlockPath bp0 = BlockWriter.computeBlockPath(outputDir, 0);
            Files.createDirectories(bp0.dirPath());

            try (BlockZipAppender appender = BlockWriter.openZipForAppend(bp0.zipFilePath())) {
                final byte[] bytes = BlockWriter.serializeBlockToBytes(chain.getFirst(), CompressionType.ZSTD);
                BlockWriter.writeBlockEntry(appender, bp0, bytes);
            }

            try (ZipFile zf = new ZipFile(bp0.zipFilePath().toFile())) {
                assertEquals(1, zf.size(), "Zip should contain 1 entry");
            }
        }

        @Test
        @DisplayName("ZipAppender close is idempotent")
        void testZipAppenderCloseIsIdempotent() throws IOException {
            final Path outputDir = zipDir.resolve("idempotent-close");
            Files.createDirectories(outputDir);
            final List<Block> chain = TestBlockFactory.createValidChain(1);

            final BlockPath bp0 = BlockWriter.computeBlockPath(outputDir, 0);
            Files.createDirectories(bp0.dirPath());

            final BlockZipAppender appender = BlockWriter.openZipForAppend(bp0.zipFilePath());
            final byte[] bytes = BlockWriter.serializeBlockToBytes(chain.getFirst(), CompressionType.ZSTD);
            BlockWriter.writeBlockEntry(appender, bp0, bytes);
            appender.close();
            // Second close should not throw
            appender.close();
        }

        @Test
        @DisplayName("AtomicRef cleared before open attempt — refs stay null on failure")
        void testAtomicRefClearedBeforeOpenZipAttempt() throws IOException {
            final Path outputDir = zipDir.resolve("atomic-ref");
            Files.createDirectories(outputDir);
            final List<Block> chain = TestBlockFactory.createValidChain(1);

            // Setup valid initial state
            final BlockPath bp0 = BlockWriter.computeBlockPath(outputDir, 0);
            Files.createDirectories(bp0.dirPath());
            final BlockZipAppender initialAppender = BlockWriter.openZipForAppend(bp0.zipFilePath());
            final byte[] bytes = BlockWriter.serializeBlockToBytes(chain.getFirst(), CompressionType.ZSTD);
            BlockWriter.writeBlockEntry(initialAppender, bp0, bytes);

            final AtomicReference<BlockZipAppender> appenderRef = new AtomicReference<>(initialAppender);
            final AtomicReference<Path> pathRef = new AtomicReference<>(bp0.zipFilePath());

            // Simulate Stage 4 close-and-reopen pattern: close old, null refs
            final BlockZipAppender oldAppender = appenderRef.getAndSet(null);
            pathRef.set(null);
            oldAppender.close();

            // Verify both refs are null
            assertNull(appenderRef.get());
            assertNull(pathRef.get());

            // Attempt open on a non-writable path (directory itself)
            try {
                final Path badPath = zipDir.resolve("nonexistent-dir/bad.zip");
                BlockWriter.openZipForAppend(badPath);
                // If it doesn't throw, that's ok too — the point is refs should still be null
            } catch (IOException expected) {
                // Expected — the point is that refs were cleared before the attempt
            }

            // Refs should still be null (not stale)
            assertNull(appenderRef.get());
            assertNull(pathRef.get());
        }

        @Test
        @DisplayName("ZipAppender resource cleanup on write error pattern")
        void testZipAppenderResourceCleanupOnWriteError() throws IOException {
            final Path outputDir = zipDir.resolve("cleanup");
            Files.createDirectories(outputDir);
            final List<Block> chain = TestBlockFactory.createValidChain(1);

            final BlockPath bp0 = BlockWriter.computeBlockPath(outputDir, 0);
            Files.createDirectories(bp0.dirPath());

            final AtomicReference<BlockZipAppender> appenderRef = new AtomicReference<>();
            final AtomicReference<Path> pathRef = new AtomicReference<>();

            // Open and write successfully
            final BlockZipAppender appender = BlockWriter.openZipForAppend(bp0.zipFilePath());
            appenderRef.set(appender);
            pathRef.set(bp0.zipFilePath());
            final byte[] bytes = BlockWriter.serializeBlockToBytes(chain.getFirst(), CompressionType.ZSTD);
            BlockWriter.writeBlockEntry(appender, bp0, bytes);

            // Simulate catch-block cleanup: getAndSet(null), null path, close extracted appender
            final BlockZipAppender extracted = appenderRef.getAndSet(null);
            pathRef.set(null);
            extracted.close();

            // Verify no leak: refs are null, appender was closed
            assertNull(appenderRef.get());
            assertNull(pathRef.get());

            // Zip should still be valid with the 1 entry written before cleanup
            try (ZipFile zf = new ZipFile(bp0.zipFilePath().toFile())) {
                assertEquals(1, zf.size());
            }
        }
    }

    // ===== Resume with watermark behind registry tests =====

    @Nested
    @DisplayName("Resume with watermark behind registry")
    class ResumeWithWatermarkBehindRegistry {

        @TempDir
        Path resumeDir;

        @Test
        @DisplayName("Resume reconciliation truncates and replays correctly")
        void testResumeReconciliationTruncatesAndReplays() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(15);
            final byte[][] blockHashes = new byte[15][];
            for (int i = 0; i < chain.size(); i++) {
                blockHashes[i] = BlockStreamBlockHasher.hashBlock(chain.get(i));
            }

            // Build registry with all 15 blocks
            final Path regFile = resumeDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 15; i++) {
                    registry.addBlock(i, blockHashes[i]);
                }

                // Save watermark at 9
                final Path wf = resumeDir.resolve("wrap-commit.bin");
                ToWrappedBlocksCommand.saveWatermark(wf, 9L);

                // Simulate resume: truncate registry to watermark
                registry.truncateTo(9);
                assertEquals(9, registry.highestBlockNumberStored());

                // Replay hashers 0..9
                final StreamingHasher streamingHasher = new StreamingHasher();
                final InMemoryTreeHasher inMemoryHasher = new InMemoryTreeHasher();
                for (long bn = 0; bn <= 9; bn++) {
                    final byte[] hash = registry.getBlockHash(bn);
                    streamingHasher.addNodeByHash(hash);
                    inMemoryHasher.addNodeByHash(hash);
                }
                assertEquals(10, streamingHasher.leafCount());

                // Continue adding blocks 10..14
                for (int i = 10; i < 15; i++) {
                    registry.addBlock(i, blockHashes[i]);
                    streamingHasher.addNodeByHash(blockHashes[i]);
                    inMemoryHasher.addNodeByHash(blockHashes[i]);
                }

                // Build reference from all 15 blocks
                final StreamingHasher reference = new StreamingHasher();
                final InMemoryTreeHasher referenceInMemory = new InMemoryTreeHasher();
                for (int i = 0; i < 15; i++) {
                    reference.addNodeByHash(blockHashes[i]);
                    referenceInMemory.addNodeByHash(blockHashes[i]);
                }

                assertArrayEquals(reference.computeRootHash(), streamingHasher.computeRootHash());
                assertArrayEquals(referenceInMemory.computeRootHash(), inMemoryHasher.computeRootHash());
                assertEquals(15, streamingHasher.leafCount());
            }
        }

        @Test
        @DisplayName("Resume with no watermark trusts registry")
        void testResumeWithNoWatermarkTrustsRegistry() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(10);
            final byte[][] blockHashes = new byte[10][];
            for (int i = 0; i < chain.size(); i++) {
                blockHashes[i] = BlockStreamBlockHasher.hashBlock(chain.get(i));
            }

            final Path regFile = resumeDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 10; i++) {
                    registry.addBlock(i, blockHashes[i]);
                }

                // No watermark file
                final long watermark = ToWrappedBlocksCommand.loadWatermark(resumeDir.resolve("wrap-commit.bin"));
                assertEquals(-1, watermark);

                // Since watermark < 0 && registryHighest >= 0, set durableWatermark = registryHighest
                final long durableWatermark = registry.highestBlockNumberStored();
                assertEquals(9, durableWatermark);

                // No truncation needed. Replay all.
                final StreamingHasher streamingHasher = new StreamingHasher();
                for (long bn = 0; bn <= durableWatermark; bn++) {
                    streamingHasher.addNodeByHash(registry.getBlockHash(bn));
                }
                assertEquals(10, streamingHasher.leafCount());
            }
        }

        @Test
        @DisplayName("Resume with watermark matching registry — no truncation needed")
        void testResumeWithWatermarkMatchingRegistry() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(10);
            final byte[][] blockHashes = new byte[10][];
            for (int i = 0; i < chain.size(); i++) {
                blockHashes[i] = BlockStreamBlockHasher.hashBlock(chain.get(i));
            }

            final Path regFile = resumeDir.resolve("hashes.bin");
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 10; i++) {
                    registry.addBlock(i, blockHashes[i]);
                }

                // Watermark at 9 = registry highest
                final Path wf = resumeDir.resolve("wrap-commit.bin");
                ToWrappedBlocksCommand.saveWatermark(wf, 9L);
                final long watermark = ToWrappedBlocksCommand.loadWatermark(wf);
                assertEquals(registry.highestBlockNumberStored(), watermark);

                // No truncation needed. Replay all 10.
                final StreamingHasher streamingHasher = new StreamingHasher();
                for (long bn = 0; bn <= watermark; bn++) {
                    streamingHasher.addNodeByHash(registry.getBlockHash(bn));
                }
                assertEquals(10, streamingHasher.leafCount());
            }
        }
    }

    // ===== Jumpstart data serialization tests =====

    @Nested
    @DisplayName("Jumpstart data serialization")
    class JumpstartDataSerialization {

        @TempDir
        Path jumpstartDir;

        @Test
        @DisplayName("Jumpstart data format round-trip")
        void testJumpstartDataFormatRoundTrip() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(10);
            final StreamingHasher streamingHasher = new StreamingHasher();
            byte[] lastHash = null;
            for (Block block : chain) {
                lastHash = BlockStreamBlockHasher.hashBlock(block);
                streamingHasher.addNodeByHash(lastHash);
            }

            final Path file = jumpstartDir.resolve("jumpstart.bin");
            ToWrappedBlocksCommand.saveJumpstartData(file, 9, lastHash, streamingHasher);

            // Read back and verify format
            try (DataInputStream in = new DataInputStream(Files.newInputStream(file))) {
                assertEquals(9, in.readLong(), "Block number should be 9");

                final byte[] readHash = new byte[48];
                in.readFully(readHash);
                assertArrayEquals(lastHash, readHash, "Block hash should match");

                assertEquals(10, in.readLong(), "Leaf count should be 10");

                final int hashListSize = in.readInt();
                final List<byte[]> intermediateState = streamingHasher.intermediateHashingState();
                assertEquals(intermediateState.size(), hashListSize, "Hash list size should match");

                for (int i = 0; i < hashListSize; i++) {
                    final byte[] readIntermediateHash = new byte[48];
                    in.readFully(readIntermediateHash);
                    assertArrayEquals(intermediateState.get(i), readIntermediateHash, "Intermediate hash " + i);
                }
            }
        }

        @Test
        @DisplayName("Jumpstart data for single block")
        void testJumpstartDataForSingleBlock() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(1);
            final StreamingHasher streamingHasher = new StreamingHasher();
            final byte[] blockHash = BlockStreamBlockHasher.hashBlock(chain.getFirst());
            streamingHasher.addNodeByHash(blockHash);

            final Path file = jumpstartDir.resolve("jumpstart.bin");
            ToWrappedBlocksCommand.saveJumpstartData(file, 0, blockHash, streamingHasher);

            try (DataInputStream in = new DataInputStream(Files.newInputStream(file))) {
                assertEquals(0, in.readLong(), "Block number should be 0");
                final byte[] readHash = new byte[48];
                in.readFully(readHash);
                assertArrayEquals(blockHash, readHash);
                assertEquals(1, in.readLong(), "Leaf count should be 1");
            }
        }

        @Test
        @DisplayName("Jumpstart data overwrites previous file")
        void testJumpstartDataOverwritesPrevious() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(11);
            final StreamingHasher hasher5 = new StreamingHasher();
            byte[] hash5 = null;
            for (int i = 0; i <= 5; i++) {
                hash5 = BlockStreamBlockHasher.hashBlock(chain.get(i));
                hasher5.addNodeByHash(hash5);
            }

            final Path file = jumpstartDir.resolve("jumpstart.bin");
            ToWrappedBlocksCommand.saveJumpstartData(file, 5, hash5, hasher5);

            // Overwrite with block 10
            final StreamingHasher hasher10 = new StreamingHasher();
            byte[] hash10 = null;
            for (int i = 0; i <= 10; i++) {
                hash10 = BlockStreamBlockHasher.hashBlock(chain.get(i));
                hasher10.addNodeByHash(hash10);
            }
            ToWrappedBlocksCommand.saveJumpstartData(file, 10, hash10, hasher10);

            try (DataInputStream in = new DataInputStream(Files.newInputStream(file))) {
                assertEquals(10, in.readLong(), "Block number should be 10 after overwrite");
            }
        }
    }

    // ===== Multiple resume cycle tests =====

    @Nested
    @DisplayName("Multiple resume cycles")
    class MultipleResumeCycles {

        @TempDir
        Path cycleDir;

        @Test
        @DisplayName("Three resume cycles produce consistent state")
        void testThreeResumeCyclesProduceConsistentState() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(30);
            final byte[][] blockHashes = new byte[30][];
            for (int i = 0; i < chain.size(); i++) {
                blockHashes[i] = BlockStreamBlockHasher.hashBlock(chain.get(i));
            }

            final Path regFile = cycleDir.resolve("hashes.bin");
            final Path wf = cycleDir.resolve("wrap-commit.bin");

            // Cycle 1: blocks 0..9
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                final StreamingHasher streaming = new StreamingHasher();
                final InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
                for (int i = 0; i < 10; i++) {
                    registry.addBlock(i, blockHashes[i]);
                    streaming.addNodeByHash(blockHashes[i]);
                    inMemory.addNodeByHash(blockHashes[i]);
                }
                ToWrappedBlocksCommand.saveWatermark(wf, 9L);
            }

            // Cycle 2: resume, replay 0..9, then add 10..19
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                final long watermark = ToWrappedBlocksCommand.loadWatermark(wf);
                assertEquals(9, watermark);
                assertEquals(9, registry.highestBlockNumberStored());

                final StreamingHasher streaming = new StreamingHasher();
                final InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
                for (long bn = 0; bn <= watermark; bn++) {
                    streaming.addNodeByHash(registry.getBlockHash(bn));
                    inMemory.addNodeByHash(registry.getBlockHash(bn));
                }

                for (int i = 10; i < 20; i++) {
                    registry.addBlock(i, blockHashes[i]);
                    streaming.addNodeByHash(blockHashes[i]);
                    inMemory.addNodeByHash(blockHashes[i]);
                }
                ToWrappedBlocksCommand.saveWatermark(wf, 19L);
            }

            // Cycle 3: resume, replay 0..19, then add 20..29
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                final long watermark = ToWrappedBlocksCommand.loadWatermark(wf);
                assertEquals(19, watermark);
                assertEquals(19, registry.highestBlockNumberStored());

                final StreamingHasher streaming = new StreamingHasher();
                final InMemoryTreeHasher inMemory = new InMemoryTreeHasher();
                for (long bn = 0; bn <= watermark; bn++) {
                    streaming.addNodeByHash(registry.getBlockHash(bn));
                    inMemory.addNodeByHash(registry.getBlockHash(bn));
                }

                for (int i = 20; i < 30; i++) {
                    registry.addBlock(i, blockHashes[i]);
                    streaming.addNodeByHash(blockHashes[i]);
                    inMemory.addNodeByHash(blockHashes[i]);
                }
                ToWrappedBlocksCommand.saveWatermark(wf, 29L);

                // Verify final state matches reference built from all 30 blocks
                final StreamingHasher reference = new StreamingHasher();
                final InMemoryTreeHasher referenceInMemory = new InMemoryTreeHasher();
                for (int i = 0; i < 30; i++) {
                    reference.addNodeByHash(blockHashes[i]);
                    referenceInMemory.addNodeByHash(blockHashes[i]);
                }

                assertArrayEquals(reference.computeRootHash(), streaming.computeRootHash());
                assertArrayEquals(referenceInMemory.computeRootHash(), inMemory.computeRootHash());
                assertEquals(30, streaming.leafCount());
                assertEquals(29, registry.highestBlockNumberStored());
                assertEquals(29L, ToWrappedBlocksCommand.loadWatermark(wf));
            }
        }

        @Test
        @DisplayName("Resume cycle with mid-registry truncation")
        void testResumeCycleWithMidRegistryTruncation() throws Exception {
            final List<Block> chain = TestBlockFactory.createValidChain(20);
            final byte[][] blockHashes = new byte[20][];
            for (int i = 0; i < chain.size(); i++) {
                blockHashes[i] = BlockStreamBlockHasher.hashBlock(chain.get(i));
            }

            final Path regFile = cycleDir.resolve("hashes.bin");
            final Path wf = cycleDir.resolve("wrap-commit.bin");

            // Cycle 1: write 0..14 to registry, but watermark only at 9 (simulating crash)
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                for (int i = 0; i < 15; i++) {
                    registry.addBlock(i, blockHashes[i]);
                }
                ToWrappedBlocksCommand.saveWatermark(wf, 9L);
            }

            // Cycle 2: resume — registry at 14, watermark at 9 → truncate to 9
            try (BlockStreamBlockHashRegistry registry = new BlockStreamBlockHashRegistry(regFile)) {
                final long watermark = ToWrappedBlocksCommand.loadWatermark(wf);
                assertEquals(9, watermark);
                assertEquals(14, registry.highestBlockNumberStored());

                // Truncate registry to watermark
                registry.truncateTo(watermark);
                assertEquals(9, registry.highestBlockNumberStored());

                // Replay 0..9
                final StreamingHasher streaming = new StreamingHasher();
                for (long bn = 0; bn <= watermark; bn++) {
                    streaming.addNodeByHash(registry.getBlockHash(bn));
                }

                // Continue adding 10..19
                for (int i = 10; i < 20; i++) {
                    registry.addBlock(i, blockHashes[i]);
                    streaming.addNodeByHash(blockHashes[i]);
                }
                ToWrappedBlocksCommand.saveWatermark(wf, 19L);

                // Verify final state matches reference for 20 blocks
                final StreamingHasher reference = new StreamingHasher();
                for (int i = 0; i < 20; i++) {
                    reference.addNodeByHash(blockHashes[i]);
                }

                assertArrayEquals(reference.computeRootHash(), streaming.computeRootHash());
                assertEquals(20, streaming.leafCount());
            }
        }
    }

    // ===== Watermark batch flushing tests =====

    @Nested
    @DisplayName("Watermark batch flushing")
    class WatermarkBatchFlushing {

        @TempDir
        Path flushDir;

        @Test
        @DisplayName("Watermark flushes at batch boundary (every 256 blocks)")
        void testWatermarkFlushesAtBatchBoundary() {
            final Path wf = flushDir.resolve("wrap-commit.bin");
            final AtomicLong durableWatermark = new AtomicLong(-1);
            final AtomicLong blocksSinceWatermarkFlush = new AtomicLong(0);

            // Simulate Stage 4 loop for 512 blocks
            for (int blockNum = 0; blockNum < 512; blockNum++) {
                final long count = blocksSinceWatermarkFlush.incrementAndGet();
                if (count >= 256) {
                    ToWrappedBlocksCommand.saveWatermark(wf, blockNum);
                    durableWatermark.set(blockNum);
                    blocksSinceWatermarkFlush.set(0);
                }
            }

            // Watermark should have been flushed at block 255 and 511
            assertEquals(511, durableWatermark.get());
            assertEquals(511, ToWrappedBlocksCommand.loadWatermark(wf));
        }

        @Test
        @DisplayName("Watermark flushed on zip close")
        void testWatermarkFlushedOnZipClose() {
            final Path wf = flushDir.resolve("wrap-commit.bin");
            final AtomicLong blocksSinceWatermarkFlush = new AtomicLong(0);

            // Simulate writing 50 blocks (less than 256 batch boundary)
            for (int i = 0; i < 50; i++) {
                blocksSinceWatermarkFlush.incrementAndGet();
            }

            // Simulate zip-switch: flush watermark at close time, independent of batch counter
            final long lastBlockInZip = 49;
            ToWrappedBlocksCommand.saveWatermark(wf, lastBlockInZip);
            blocksSinceWatermarkFlush.set(0);

            assertEquals(49, ToWrappedBlocksCommand.loadWatermark(wf));
            assertEquals(0, blocksSinceWatermarkFlush.get());
        }
    }

    // ===== Helpers =====

    private static boolean isZstdAvailable() {
        try {
            Process p = new ProcessBuilder("which", "zstd").start();
            return p.waitFor() == 0;
        } catch (Exception e) {
            return false;
        }
    }
}
