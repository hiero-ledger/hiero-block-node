// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.wrapped;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;

import com.hedera.hapi.block.stream.Block;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.model.BlockReader;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * CLI subcommand that validates wrapped block stream files produced by
 * {@link org.hiero.block.tools.blocks.ToWrappedBlocksCommand}.
 *
 * <h2>Two-Stage Parallel Pipeline</h2>
 *
 * <p>Validation uses a two-stage pipeline to maximise throughput:
 *
 * <pre>
 * Stage 1 — Read + Parse + Stateless Checks  [readAndParsePool, N threads, sliding prefetch window]
 *
 *   For each block in the prefetch window:
 *     1. BlockReader.readBlock(inputDir, blockNumber)  — zip I/O + decompress + protobuf parse
 *     2. WrappedBlockValidator.validateRequiredItems   — stateless, thread-safe
 *     3. WrappedBlockValidator.validateNoExtraItems    — stateless, thread-safe
 *     -> CompletableFuture<Block>
 *
 *     future.join() — main thread waits for the oldest in-flight future
 *     |
 *     v
 * Stage 2 — Sequential Chain-State Checks  [main thread, strictly sequential]
 *
 *   WrappedBlockValidator.validateBlockChain            — needs hash of previous block
 *   WrappedBlockValidator.validateHistoricalBlockTreeRoot — needs StreamingHasher state
 *   WrappedBlockValidator.validate50Billion             — needs the accumulated balance map
 *   hashBlock(block) -> previousBlockHash               — feeds both checks for the next block
 *   streamingHasher.addNodeByHash(previousBlockHash)    — NOT thread-safe; main thread only
 * </pre>
 *
 * <h2>ZIP File-System Cache</h2>
 *
 * <p>Each zip file holds up to 10,000 blocks. Without a cache, every {@code readBlock} call
 * re-opens the same zip, re-parsing its central directory. {@link BlockReader} caches open
 * {@code FileSystem} objects across calls; {@link BlockReader#closeZipCache()} is called in
 * the {@code finally} block to release file handles when validation finishes.
 *
 * <h2>CLI Options</h2>
 * <ul>
 *   <li>{@code --threads} — thread count for Stage 1 (default: CPU count - 1)</li>
 *   <li>{@code --prefetch} — sliding window size (default: threads × 2)</li>
 *   <li>{@code --start-block} — first block to validate (default: first stored block)</li>
 *   <li>{@code --end-block} — last block to validate (default: last stored block)</li>
 *   <li>{@code --skip-50b} — skip the 50-billion HBAR supply check</li>
 * </ul>
 */
@SuppressWarnings({"FieldCanBeLocal", "DuplicatedCode"})
@Command(
        name = "validate-wrapped",
        description = "Validate wrapped block stream blocks produced by the wrap command",
        mixinStandardHelpOptions = true)
public class ValidateWrappedBlocksCommand implements Callable<Integer> {

    @SuppressWarnings("unused")
    @Parameters(index = "0..1", description = "Block files, directories, or zip archives to process")
    private File[] files;

    @Option(
            names = {"--threads"},
            description = "Thread count for the read + parse + stateless-validate stage. Default: CPU count minus 1")
    private int threads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    @Option(
            names = {"--prefetch"},
            description =
                    "Number of read+parse futures to keep in-flight ahead of the chain-check thread. Default: threads × 2")
    private int prefetch = 0;

    @Option(
            names = {"--start-block"},
            description = "First block number to validate (inclusive). Default: first stored block")
    private long startBlock = -1;

    @Option(
            names = {"--end-block"},
            description = "Last block number to validate (inclusive). Default: last stored block")
    private long endBlock = -1;

    @Option(
            names = {"--skip-50b"},
            description = "Skip the 50-billion HBAR supply check (faster for structure-only runs)")
    private boolean skip50b = false;

    @Override
    public Integer call() {
        if (files == null || files.length == 0) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No input directory specified"));
            return 1;
        }
        final Path inputDir = files[0].toPath();
        if (!Files.isDirectory(inputDir)) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ Input directory does not exist: " + inputDir));
            return 1;
        }

        // Discover stored block range — try zip archives first, then individual files, ZSTD then NONE
        long firstBlock = BlockWriter.minStoredBlockNumber(inputDir, CompressionType.ZSTD);
        long lastBlock = BlockWriter.maxStoredBlockNumber(inputDir, CompressionType.ZSTD);
        if (firstBlock < 0) {
            firstBlock = BlockWriter.minStoredBlockNumber(inputDir, CompressionType.NONE);
            lastBlock = BlockWriter.maxStoredBlockNumber(inputDir, CompressionType.NONE);
        }
        if (firstBlock < 0) {
            firstBlock = BlockFile.nestedDirectoriesMinBlockNumber(inputDir, CompressionType.ZSTD);
            lastBlock = BlockFile.nestedDirectoriesMaxBlockNumber(inputDir, CompressionType.ZSTD);
        }
        if (firstBlock < 0) {
            firstBlock = BlockFile.nestedDirectoriesMinBlockNumber(inputDir, CompressionType.NONE);
            lastBlock = BlockFile.nestedDirectoriesMaxBlockNumber(inputDir, CompressionType.NONE);
        }
        if (firstBlock < 0 || lastBlock < 0) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ No blocks found in: " + inputDir.toAbsolutePath()));
            return 1;
        }

        // Apply --start-block / --end-block overrides
        final long effectiveFirstBlock = (startBlock >= 0) ? startBlock : firstBlock;
        final long effectiveLastBlock = (endBlock >= 0) ? endBlock : lastBlock;
        if (effectiveFirstBlock > effectiveLastBlock) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ --start-block (" + effectiveFirstBlock
                    + ") > --end-block (" + effectiveLastBlock + ")"));
            return 1;
        }

        final long totalBlocks = effectiveLastBlock - effectiveFirstBlock + 1;
        final boolean startsAtZero = effectiveFirstBlock == 0;
        final int resolvedThreads = threads;
        final int resolvedPrefetch = (prefetch > 0) ? prefetch : resolvedThreads * 2;

        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   WRAPPED BLOCK VALIDATION|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|yellow Input directory:|@ " + inputDir.toAbsolutePath()));
        System.out.println(Ansi.AUTO.string("@|yellow Stored block range:|@ " + firstBlock + " - " + lastBlock));
        System.out.println(
                Ansi.AUTO.string("@|yellow Validating range:|@  " + effectiveFirstBlock + " - " + effectiveLastBlock));
        System.out.println(Ansi.AUTO.string("@|yellow Total blocks:|@ " + totalBlocks));
        System.out.println(
                Ansi.AUTO.string("@|yellow Threads:|@ " + resolvedThreads + "  prefetch window: " + resolvedPrefetch));
        if (startsAtZero) {
            System.out.println(
                    Ansi.AUTO.string("@|yellow Historical block hash tree validation:|@ enabled (starts at block 0)"));
            System.out.println(Ansi.AUTO.string(
                    "@|yellow 50-billion HBAR supply check:|@ " + (skip50b ? "disabled (--skip-50b)" : "enabled")));
        } else {
            System.out.println(
                    Ansi.AUTO.string("@|yellow Historical block hash tree validation:|@ disabled (starts at block "
                            + effectiveFirstBlock + ")"));
            System.out.println(Ansi.AUTO.string("@|yellow 50-billion HBAR supply check:|@ disabled (not at block 0)"));
        }
        System.out.println();

        // Create streaming hasher and balance map only when starting from block 0
        final StreamingHasher streamingHasher = startsAtZero ? new StreamingHasher() : null;
        final Map<Long, Long> balanceMap = (startsAtZero && !skip50b) ? new HashMap<>() : null;

        final long startNanos = System.nanoTime();
        long blocksValidated = 0;
        byte[] previousBlockHash = null;

        final ExecutorService readAndParsePool = Executors.newFixedThreadPool(resolvedThreads);
        // Sliding window of Stage 1 (read + parse + stateless-check) futures
        final Deque<CompletableFuture<Block>> window = new ArrayDeque<>();
        long nextToFetch = effectiveFirstBlock;
        long nextToValidate = effectiveFirstBlock;
        long lastProgressNanos = System.nanoTime();

        try {
            while (nextToValidate <= effectiveLastBlock) {
                // ---- Stage 1: fill the sliding read+parse window ----
                while (window.size() < resolvedPrefetch && nextToFetch <= effectiveLastBlock) {
                    final long bn = nextToFetch++;
                    window.add(CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    final Block block = BlockReader.readBlock(inputDir, bn);
                                    WrappedBlockValidator.validateRequiredItems(bn, block);
                                    WrappedBlockValidator.validateNoExtraItems(bn, block);
                                    return block;
                                } catch (IOException | ValidationException e) {
                                    throw new RuntimeException("Block " + bn + ": " + e.getMessage(), e);
                                }
                            },
                            readAndParsePool));
                }
                if (window.isEmpty()) {
                    break;
                }

                // ---- Stage 2: sequential chain-state checks (main thread) ----
                // Wait for the oldest in-flight read+parse future.
                final Block block;
                try {
                    block = window.poll().join();
                } catch (CompletionException ex) {
                    PrettyPrint.clearProgress();
                    final Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                    System.err.println(Ansi.AUTO.string("@|red Error:|@ " + cause.getMessage()));
                    return 1;
                }

                final long bn = nextToValidate++;
                try {
                    WrappedBlockValidator.validateBlockChain(bn, block, previousBlockHash);
                    WrappedBlockValidator.validateHistoricalBlockTreeRoot(bn, block, streamingHasher);
                    if (balanceMap != null) {
                        WrappedBlockValidator.validate50Billion(bn, block, balanceMap);
                    }
                } catch (ValidationException e) {
                    PrettyPrint.clearProgress();
                    System.err.println(Ansi.AUTO.string("@|red Block " + bn + ":|@ " + e.getMessage()));
                    return 1;
                }

                // Update chain state (not thread-safe — must stay on main thread)
                previousBlockHash = hashBlock(block);
                if (streamingHasher != null) {
                    streamingHasher.addNodeByHash(previousBlockHash);
                }
                blocksValidated++;

                // Time-based progress: print once per ~5 seconds or on the last block
                final long now = System.nanoTime();
                if (now - lastProgressNanos >= 5_000_000_000L || bn == effectiveLastBlock) {
                    final long elapsedMillis = (now - startNanos) / 1_000_000L;
                    final long remainingMillis =
                            PrettyPrint.computeRemainingMilliseconds(blocksValidated, totalBlocks, elapsedMillis);
                    final double percent = ((double) blocksValidated / (double) totalBlocks) * 100.0;
                    final String progressString = String.format("Validated %d/%d blocks", blocksValidated, totalBlocks);
                    PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
                    lastProgressNanos = now;
                }
            }
        } finally {
            readAndParsePool.shutdownNow();
            BlockReader.closeZipCache();
        }

        // Print summary
        PrettyPrint.clearProgress();
        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   VALIDATION SUMMARY|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|yellow Blocks validated:|@ " + blocksValidated));

        if (blocksValidated == 0) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|bold,yellow No blocks found in:|@ " + inputDir.toAbsolutePath()));
        }

        final long elapsedSeconds = (System.nanoTime() - startNanos) / 1_000_000_000L;
        System.out.println(Ansi.AUTO.string("@|yellow Time elapsed:|@ " + elapsedSeconds + " seconds"));
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|bold,green VALIDATION PASSED|@"));
        return 0;
    }
}
