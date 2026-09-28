// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.blocks.files.historic;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.node.app.fixtures.plugintest.TestHealthFacility;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.historicalblocks.BlockAccessor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Benchmark comparing {@link ZipBlockAccessor} (default, one filesystem per accessor via a hard link) against
 * {@link CachedZipBlockAccessor} (shared, reference-counted filesystem cache). Runs as part of the normal test
 * suite; it only asserts on functional correctness (every read returns the expected block), never on timings, so
 * it cannot flake on CI hardware variance -- the comparison numbers are informational, printed to stdout and
 * written to {@link #REPORT_PATH} for humans to read.
 */
@DisplayName("Benchmark: ZipBlockAccessor vs CachedZipBlockAccessor performance")
class AccessorPerformanceComparisonTest {

    /** Where to write the human-readable report. Override with -DaccessorBenchmarkReport=<path>. */
    private static final Path REPORT_PATH =
            Path.of(System.getProperty("accessorBenchmarkReport", "build/reports/accessor-benchmark/results.txt"));

    /** Number of blocks written into the single archive under test; all land in one zip file. */
    private static final int BLOCKS_PER_ARCHIVE = 2000;

    @Test
    @DisplayName("Compare sequential-chunk, random-access, and concurrent-access performance")
    void compareImplementations() throws Exception {
        final Path benchRoot = Files.createTempDirectory("bn-accessor-bench");
        try {
            final FilesHistoricConfig legacyConfig = configFor(benchRoot, false);
            final FilesHistoricConfig cachedConfig = configFor(benchRoot, true);
            final BlockNodeContext context = minimalContext();

            writeArchive(legacyConfig, BLOCKS_PER_ARCHIVE);

            final ZipBlockArchive legacyArchive = new ZipBlockArchive(context, legacyConfig);
            final ZipBlockArchive cachedArchive = new ZipBlockArchive(context, cachedConfig);

            final StringBuilder report = new StringBuilder();
            report.append("Accessor performance comparison\n");
            report.append("blocksPerArchive=").append(BLOCKS_PER_ARCHIVE).append("\n\n");

            // Warm up JIT / page cache for both paths before timing anything.
            runSequentialChunks(legacyArchive, 10, 30);
            runSequentialChunks(cachedArchive, 10, 30);
            runRandomAccess(legacyArchive, 500, 1);
            runRandomAccess(cachedArchive, 500, 1);

            report.append("== Sequential chunk reads (simulates a backfill chunk fetch from one archive) ==\n");
            for (final int chunkSize : new int[] {1, 10, 100}) {
                final int repetitions = 100;
                final long legacyNanos = runSequentialChunks(legacyArchive, chunkSize, repetitions);
                final long cachedNanos = runSequentialChunks(cachedArchive, chunkSize, repetitions);
                appendComparison(
                        report,
                        "chunkSize=%-4d reps=%d".formatted(chunkSize, repetitions),
                        legacyNanos,
                        cachedNanos,
                        repetitions);
            }

            report.append("\n== Single-threaded random access across the archive ==\n");
            final int randomOps = 3000;
            final long legacyRandomNanos = runRandomAccess(legacyArchive, randomOps, 42);
            final long cachedRandomNanos = runRandomAccess(cachedArchive, randomOps, 42);
            appendComparison(
                    report, "randomOps=%d".formatted(randomOps), legacyRandomNanos, cachedRandomNanos, randomOps);

            report.append("\n== Concurrent random access (total wall-clock for all threads) ==\n");
            for (final int threadCount : new int[] {4, 16}) {
                final int opsPerThread = 500;
                final long legacyNanos = runConcurrentRandomAccess(legacyArchive, threadCount, opsPerThread);
                final long cachedNanos = runConcurrentRandomAccess(cachedArchive, threadCount, opsPerThread);
                appendComparison(
                        report,
                        "threads=%-2d opsPerThread=%d".formatted(threadCount, opsPerThread),
                        legacyNanos,
                        cachedNanos,
                        1);
            }

            Files.createDirectories(REPORT_PATH.getParent());
            Files.writeString(REPORT_PATH, report.toString());
            System.out.println(report);
        } finally {
            deleteRecursively(benchRoot);
        }
    }

    private void appendComparison(
            final StringBuilder report,
            final String label,
            final long legacyNanos,
            final long cachedNanos,
            final int divisor) {
        final double legacyPer = legacyNanos / (double) divisor / 1_000_000.0;
        final double cachedPer = cachedNanos / (double) divisor / 1_000_000.0;
        final double speedup = legacyNanos / (double) cachedNanos;
        report.append("%-28s legacy=%10.4f ms  cached=%10.4f ms  speedup=%.2fx%n"
                .formatted(label, legacyPer, cachedPer, speedup));
    }

    /**
     * Reads {@code repetitions} chunks of {@code chunkSize} consecutive blocks starting at a random offset within
     * the archive, closing each accessor after reading its bytes (mirrors how BlockStreamSubscriberSession reads
     * one block at a time within a requested range). Returns total elapsed nanos.
     */
    private long runSequentialChunks(final ZipBlockArchive archive, final int chunkSize, final int repetitions)
            throws IOException {
        final Random random = new Random(1);
        long totalNanos = 0;
        for (int r = 0; r < repetitions; r++) {
            final long start = random.nextInt(BLOCKS_PER_ARCHIVE - chunkSize);
            final long startNanos = System.nanoTime();
            for (long blockNumber = start; blockNumber < start + chunkSize; blockNumber++) {
                try (final BlockAccessor accessor = archive.blockAccessor(blockNumber)) {
                    assertNotNull(accessor);
                    consume(accessor);
                }
            }
            totalNanos += System.nanoTime() - startNanos;
        }
        return totalNanos;
    }

    /** Reads {@code ops} single blocks at uniformly random offsets within the archive. Returns total elapsed nanos. */
    private long runRandomAccess(final ZipBlockArchive archive, final int ops, final long seed) {
        final Random random = new Random(seed);
        long totalNanos = 0;
        for (int i = 0; i < ops; i++) {
            final long blockNumber = random.nextInt(BLOCKS_PER_ARCHIVE);
            final long startNanos = System.nanoTime();
            try (final BlockAccessor accessor = archive.blockAccessor(blockNumber)) {
                assertNotNull(accessor);
                consume(accessor);
            }
            totalNanos += System.nanoTime() - startNanos;
        }
        return totalNanos;
    }

    /** Runs {@code opsPerThread} random single-block reads on each of {@code threadCount} threads concurrently. */
    private long runConcurrentRandomAccess(final ZipBlockArchive archive, final int threadCount, final int opsPerThread)
            throws Exception {
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            final List<Callable<Void>> tasks = new ArrayList<>();
            for (int t = 0; t < threadCount; t++) {
                final long seed = t;
                tasks.add(() -> {
                    final Random random = new Random(seed);
                    for (int i = 0; i < opsPerThread; i++) {
                        final long blockNumber = random.nextInt(BLOCKS_PER_ARCHIVE);
                        try (final BlockAccessor accessor = archive.blockAccessor(blockNumber)) {
                            assertNotNull(accessor);
                            consume(accessor);
                        }
                    }
                    return null;
                });
            }
            final long startNanos = System.nanoTime();
            final List<Future<Void>> futures = executor.invokeAll(tasks, 60, TimeUnit.SECONDS);
            for (final Future<Void> future : futures) {
                future.get();
            }
            return System.nanoTime() - startNanos;
        } finally {
            executor.shutdownNow();
        }
    }

    /** Forces the accessor to actually do its I/O + decompression, rather than optimizing the read away. */
    private void consume(final BlockAccessor accessor) {
        final var unparsed = accessor.blockUnparsed();
        if (unparsed == null || unparsed.blockItems().isEmpty()) {
            throw new IllegalStateException("Benchmark read an empty/missing block");
        }
    }

    private FilesHistoricConfig configFor(final Path root, final boolean cachedZipAccessorEnabled) {
        return new FilesHistoricConfig(
                root,
                CompressionType.NONE,
                4, // powersOfTenPerZipFileContents: 10,000 blocks per zip, matches production default
                0L,
                3,
                false,
                cachedZipAccessorEnabled,
                8);
    }

    private BlockNodeContext minimalContext() {
        return new BlockNodeContext(
                null,
                null,
                new TestHealthFacility(),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new ArrayList<>(),
                new ArrayList<>());
    }

    /** Writes blocks {@code 0..blockCount-1} directly into the single zip archive computed for block 0. */
    private void writeArchive(@NonNull final FilesHistoricConfig config, final int blockCount) throws IOException {
        final BlockPath firstBlockPath = BlockPath.computeBlockPath(config, 0);
        Files.createDirectories(firstBlockPath.dirPath());
        try (final ZipOutputStream zipOut = new ZipOutputStream(Files.newOutputStream(firstBlockPath.zipFilePath()))) {
            for (long blockNumber = 0; blockNumber < blockCount; blockNumber++) {
                final BlockPath blockPath = BlockPath.computeBlockPath(config, blockNumber);
                final byte[] bytesToWrite = TestBlockBuilder.generateBlockWithNumber(blockNumber)
                        .bytes()
                        .toByteArray();
                final ZipEntry zipEntry = new ZipEntry(blockPath.blockFileName());
                zipOut.putNextEntry(zipEntry);
                zipOut.write(bytesToWrite);
                zipOut.closeEntry();
            }
        }
    }

    private void deleteRecursively(final Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        Files.walkFileTree(root, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
