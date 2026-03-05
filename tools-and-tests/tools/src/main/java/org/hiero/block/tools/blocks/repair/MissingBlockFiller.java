// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks.repair;

import static org.hiero.block.tools.blocks.AmendmentProvider.createAmendmentProvider;
import static org.hiero.block.tools.blocks.HasherStateFiles.loadWithFallback;
import static org.hiero.block.tools.blocks.model.BlockWriter.DEFAULT_COMPRESSION;
import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.hiero.block.tools.blocks.AmendmentProvider;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockPath;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockZipAppender;
import org.hiero.block.tools.blocks.model.PreVerifiedBlock;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.days.model.TarZstdDayUtils;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.model.parsed.ParsedRecordBlock;
import org.hiero.block.tools.records.model.parsed.RecordBlockConverter;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Help.Ansi;

/**
 * Phase 2 engine: scans a wrapped-block directory for zip archives whose block range is fully
 * covered by the hash registry but whose entry count is below the expected maximum, then
 * re-wraps each missing block from its original source record-file day archive.
 *
 * <p>Because the block hash registry ({@code blockStreamBlockHashes.bin}) stores a SHA-384 hash
 * for every block ever processed, missing blocks can be reconstructed at any point without
 * reprocessing from block zero — the streaming Merkle-tree hasher is fast-replayed from a
 * checkpoint to the first affected block, then advanced one block at a time while re-wrapping.</p>
 */
@SuppressWarnings("CallToPrintStackTrace")
public class MissingBlockFiller {

    /** Blocks per zip file — matches {@link BlockWriter#DEFAULT_POWERS_OF_TEN_PER_ZIP} = 4 (10 000 blocks). */
    private static final long BLOCKS_PER_ZIP = (long) Math.pow(10, BlockWriter.DEFAULT_POWERS_OF_TEN_PER_ZIP);

    /** File extension used for block entries inside zip files (e.g., {@code ".blk.zstd"}). */
    private static final String BLOCK_FILE_SUFFIX = ".blk" + DEFAULT_COMPRESSION.extension();

    /** Hex formatter for diagnostic hash output. */
    private static final HexFormat HEX = HexFormat.of();

    /** Minimum nanoseconds between progress-bar updates (500 ms). */
    private static final long PROGRESS_INTERVAL_NS = 500_000_000L;

    // ── Configuration ─────────────────────────────────────────────────────────────────────────────

    private final Path outputDir;
    private final Path compressedDaysDir;
    private final Path blockTimesFile;
    private final Path dayBlocksFile;
    private final String network;
    private final boolean dryRun;

    /**
     * Construct a filler with all required path and option parameters.
     *
     * @param outputDir        the wrapped-block root directory (contains zip files and the registry)
     * @param compressedDaysDir directory containing source {@code .tar.zstd} day archives
     * @param blockTimesFile   path to the {@code block_times.bin} metadata file
     * @param dayBlocksFile    path to the {@code day_blocks.json} metadata file
     * @param network          network name for amendments ({@code "mainnet"}, {@code "testnet"}, {@code "none"})
     * @param dryRun           when {@code true}, report missing blocks without modifying any files
     */
    public MissingBlockFiller(
            final Path outputDir,
            final Path compressedDaysDir,
            final Path blockTimesFile,
            final Path dayBlocksFile,
            final String network,
            final boolean dryRun) {
        this.outputDir = outputDir;
        this.compressedDaysDir = compressedDaysDir;
        this.blockTimesFile = blockTimesFile;
        this.dayBlocksFile = dayBlocksFile;
        this.network = network;
        this.dryRun = dryRun;
    }

    // ── Public entry point ────────────────────────────────────────────────────────────────────────

    /**
     * Run the fill phase.
     *
     * <p>Validates prerequisites, scans for incomplete zips, then re-wraps and appends each
     * missing block from its original source day archive. Prints progress and a summary banner
     * to {@link System#out}.</p>
     *
     * @return {@code 0} on full success, {@code 1} if any block could not be filled
     */
    public int fill() {
        if (!Files.exists(blockTimesFile) || !Files.exists(dayBlocksFile)) {
            System.err.println("""
                    Missing metadata files (block_times.bin or day_blocks.json).
                    Run: mirror fetchRecordsCsv && mirror extractBlockTimes && mirror extractDayBlock
                    """);
            return 1;
        }
        if (!dryRun && !Files.exists(compressedDaysDir)) {
            System.err.println(
                    Ansi.AUTO.string("@|red Error:|@ source day archive directory not found: " + compressedDaysDir));
            return 1;
        }

        PrettyPrint.printBanner("ZIP REPAIR — PHASE 2: FILL MISSING BLOCKS");
        System.out.println(Ansi.AUTO.string("@|yellow Output dir:|@ " + outputDir.toAbsolutePath()));
        if (dryRun) {
            System.out.println(Ansi.AUTO.string("@|yellow Mode:|@ DRY RUN (no changes will be made)"));
        } else {
            System.out.println(Ansi.AUTO.string("@|yellow Source dir:|@ " + compressedDaysDir.toAbsolutePath()));
        }
        System.out.println();

        final Path registryPath = outputDir.resolve("blockStreamBlockHashes.bin");
        if (!Files.exists(registryPath)) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ block hash registry not found: " + registryPath));
            System.err.println("Run 'blocks wrap' first to create the registry.");
            return 1;
        }

        try (final BlockStreamBlockHashRegistry blockRegistry = new BlockStreamBlockHashRegistry(registryPath);
                final BlockTimeReader blockTimeReader = new BlockTimeReader(blockTimesFile)) {
            final long registryHighest = blockRegistry.highestBlockNumberStored();
            System.out.println(Ansi.AUTO.string("@|yellow Registry:|@ blocks 0.." + registryHighest));

            System.out.println("\nScanning zip files for incomplete archives …");
            final long scanStart = System.nanoTime();
            final TreeMap<Long, IncompleteZipInfo> zipsByFirstBlock = findIncompleteZips(outputDir, registryHighest);
            final long scanSecs = (System.nanoTime() - scanStart) / 1_000_000_000L;
            System.out.printf("  Done in %ds — %d incomplete zip(s).%n%n", scanSecs, zipsByFirstBlock.size());

            if (zipsByFirstBlock.isEmpty()) {
                System.out.println(Ansi.AUTO.string("@|bold,green All zip files are complete. Nothing to fill.|@"));
                return 0;
            }

            long totalMissing = 0;
            for (final IncompleteZipInfo info : zipsByFirstBlock.values()) {
                final long missingCount = info.lastBlock()
                        - info.firstBlock()
                        + 1
                        - info.presentBlocks().size();
                totalMissing += missingCount;
                System.out.printf(
                        "  %s  present=%,d  missing=%,d%n",
                        info.zipPath(), info.presentBlocks().size(), missingCount);
            }
            System.out.printf("%nTotal missing blocks: %,d%n%n", totalMissing);
            final long totalMissingFinal = totalMissing;

            if (dryRun) {
                System.out.println(Ansi.AUTO.string("@|yellow DRY RUN — no changes made.|@"));
                return 0;
            }

            final Map<LocalDate, DayBlockInfo> dayMap = loadDayBlockInfoMap(dayBlocksFile);
            final Path addressBookFile = outputDir.resolve("addressBookHistory.json");
            final AddressBookRegistry addressBookRegistry = Files.exists(addressBookFile)
                    ? new AddressBookRegistry(addressBookFile)
                    : new AddressBookRegistry();
            final AmendmentProvider amendmentProvider = createAmendmentProvider(network);

            final List<Path> allDayPaths = TarZstdDayUtils.sortedDayPaths(new Path[] {compressedDaysDir});
            final Map<LocalDate, Path> dateToDayPath = new HashMap<>();
            for (final Path p : allDayPaths) {
                dateToDayPath.put(
                        TarZstdDayUtils.parseDayFromFileName(p.getFileName().toString()), p);
            }

            final long lowestFirstBlock = zipsByFirstBlock.firstKey();
            final long highestLastBlock =
                    zipsByFirstBlock.lastEntry().getValue().lastBlock();
            final LocalDate firstRelevantDay =
                    blockTimeReader.getBlockLocalDateTime(lowestFirstBlock).toLocalDate();
            final LocalDate lastRelevantDay =
                    blockTimeReader.getBlockLocalDateTime(highestLastBlock).toLocalDate();
            System.out.printf("Processing days: %s to %s%n%n", firstRelevantDay, lastRelevantDay);

            final Path streamingMerkleTreeFile = outputDir.resolve("streamingMerkleTree.bin");
            final StreamingHasher streamingHasher = new StreamingHasher();
            loadWithFallback(streamingMerkleTreeFile, streamingHasher::load);

            long hasherLeafCount = streamingHasher.leafCount();
            System.out.printf("Hasher checkpoint at block %,d%n", hasherLeafCount);

            final DayBlockInfo firstDayInfo = dayMap.get(firstRelevantDay);
            if (firstDayInfo == null) {
                System.err.println("Error: no day block info for first day " + firstRelevantDay);
                return 1;
            }
            final long firstDayFirstBlock = firstDayInfo.firstBlockNumber;

            if (hasherLeafCount < firstDayFirstBlock) {
                System.out.printf(
                        "Fast-replaying %,d block hashes (blocks %,d..%,d) into hasher …%n",
                        firstDayFirstBlock - hasherLeafCount, hasherLeafCount, firstDayFirstBlock - 1);
                final long replayStart = System.nanoTime();
                for (long bn = hasherLeafCount; bn < firstDayFirstBlock; bn++) {
                    streamingHasher.addNodeByHash(blockRegistry.getBlockHash(bn));
                }
                final long replaySecs = (System.nanoTime() - replayStart) / 1_000_000_000L;
                System.out.printf("  Done in %ds.%n%n", replaySecs);
                hasherLeafCount = firstDayFirstBlock;
            }

            final AtomicInteger totalFilled = new AtomicInteger(0);
            final AtomicInteger totalFailed = new AtomicInteger(0);
            final Map<Path, BlockZipAppender> openAppenders = new HashMap<>();

            final long fillStart = System.nanoTime();
            final Thread fillProgressThread = Thread.ofVirtual().start(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    printFillProgress(
                            totalFilled.get() + totalFailed.get(), totalMissingFinal, totalFailed.get(), fillStart);
                    try {
                        Thread.sleep(PROGRESS_INTERVAL_NS / 1_000_000L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            });

            LocalDate currentDay = firstRelevantDay;
            while (!currentDay.isAfter(lastRelevantDay)) {
                final DayBlockInfo dayInfo = dayMap.get(currentDay);
                if (dayInfo == null) {
                    System.err.printf("Warning: no day block info for %s — skipping.%n", currentDay);
                    currentDay = currentDay.plusDays(1);
                    continue;
                }

                final long dayFirst = dayInfo.firstBlockNumber;
                final long dayLast = Math.min(dayInfo.lastBlockNumber, registryHighest);

                final Map.Entry<Long, IncompleteZipInfo> floorEntry = zipsByFirstBlock.floorEntry(dayLast);
                final boolean hasIncompleteOverlap =
                        floorEntry != null && floorEntry.getValue().lastBlock() >= dayFirst;

                if (!hasIncompleteOverlap) {
                    final long fromBlock = Math.max(hasherLeafCount, dayFirst);
                    for (long bn = fromBlock; bn <= dayLast; bn++) {
                        streamingHasher.addNodeByHash(blockRegistry.getBlockHash(bn));
                    }
                    hasherLeafCount = dayLast + 1;
                    currentDay = currentDay.plusDays(1);
                    continue;
                }

                final Path dayPath = dateToDayPath.get(currentDay);
                if (dayPath == null) {
                    System.err.printf(
                            "Error: source day file not found for %s — cannot re-wrap missing blocks.%n", currentDay);
                    for (final Map.Entry<Long, IncompleteZipInfo> e : zipsByFirstBlock
                            .subMap(dayFirst, true, dayLast, true)
                            .entrySet()) {
                        final IncompleteZipInfo info = e.getValue();
                        final long lo = Math.max(info.firstBlock(), dayFirst);
                        final long hi = Math.min(info.lastBlock(), dayLast);
                        for (long bn = lo; bn <= hi; bn++) {
                            if (!info.presentBlocks().contains(bn)) {
                                totalFailed.incrementAndGet();
                            }
                        }
                    }
                    final long fromBlock = Math.max(hasherLeafCount, dayFirst);
                    for (long bn = fromBlock; bn <= dayLast; bn++) {
                        streamingHasher.addNodeByHash(blockRegistry.getBlockHash(bn));
                    }
                    hasherLeafCount = dayLast + 1;
                    currentDay = currentDay.plusDays(1);
                    continue;
                }

                PrettyPrint.clearProgress();
                System.out.printf(
                        "Processing day %s (%s) blocks %,d..%,d …%n",
                        currentDay, dayPath.getFileName(), dayFirst, dayLast);

                long blockNumInDay = dayFirst;
                try (Stream<UnparsedRecordBlock> dayStream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath)) {
                    for (final UnparsedRecordBlock unparsed : (Iterable<UnparsedRecordBlock>) dayStream::iterator) {
                        final long blockNum = blockNumInDay++;

                        while (hasherLeafCount < blockNum) {
                            streamingHasher.addNodeByHash(blockRegistry.getBlockHash(hasherLeafCount));
                            hasherLeafCount++;
                        }

                        final Map.Entry<Long, IncompleteZipInfo> zipEntry = zipsByFirstBlock.floorEntry(blockNum);
                        final boolean isMissing = zipEntry != null
                                && blockNum <= zipEntry.getValue().lastBlock()
                                && !zipEntry.getValue().presentBlocks().contains(blockNum);

                        if (!isMissing) {
                            if (blockNum <= registryHighest) {
                                streamingHasher.addNodeByHash(blockRegistry.getBlockHash(blockNum));
                                hasherLeafCount = blockNum + 1;
                            }
                            continue;
                        }

                        final ParsedRecordBlock parsed = unparsed.parse();
                        final NodeAddressBook ab = addressBookRegistry.getAddressBookForBlock(parsed.blockTime());
                        final byte[] signedHash = parsed.recordFile().signedHash();
                        final List<RecordFileSignature> sigs = parsed.signatureFiles().stream()
                                .parallel()
                                .filter(psf -> psf.isValid(signedHash, ab))
                                .map(psf -> psf.toRecordFileSignature(ab))
                                .toList();
                        final PreVerifiedBlock preVerified = new PreVerifiedBlock(parsed, ab, sigs);

                        final byte[] prevHash =
                                blockNum == 0 ? EMPTY_TREE_HASH : blockRegistry.getBlockHash(blockNum - 1);
                        final byte[] treeRoot = streamingHasher.computeRootHash();

                        final Block wrapped = RecordBlockConverter.toBlock(
                                preVerified, blockNum, prevHash, treeRoot, amendmentProvider);

                        final byte[] recomputedHash = hashBlock(wrapped);
                        final byte[] registryHash = blockRegistry.getBlockHash(blockNum);
                        if (!Arrays.equals(recomputedHash, registryHash)) {
                            System.err.printf(
                                    "ERROR: hash mismatch for block %,d!%n"
                                            + "  Registry: %s%n"
                                            + "  Computed: %s%n"
                                            + "  Skipping this block.%n",
                                    blockNum, HEX.formatHex(registryHash), HEX.formatHex(recomputedHash));
                            totalFailed.incrementAndGet();
                            streamingHasher.addNodeByHash(registryHash);
                            hasherLeafCount = blockNum + 1;
                            continue;
                        }

                        final byte[] blockBytes = BlockWriter.serializeBlockToBytes(wrapped, DEFAULT_COMPRESSION);
                        final BlockPath blockPath =
                                BlockWriter.computeBlockPath(outputDir, blockNum, BlockArchiveType.UNCOMPRESSED_ZIP);
                        final Path zipFilePath = blockPath.zipFilePath();
                        final BlockZipAppender appender = openAppenders.computeIfAbsent(zipFilePath, p -> {
                            try {
                                return BlockWriter.openZipForAppend(p);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                        BlockWriter.writeBlockEntry(appender, blockPath, blockBytes);

                        streamingHasher.addNodeByHash(registryHash);
                        hasherLeafCount = blockNum + 1;
                        totalFilled.incrementAndGet();

                        final IncompleteZipInfo info = zipEntry.getValue();
                        if (blockNum == info.lastBlock()) {
                            closeAppender(openAppenders, zipFilePath);
                        }
                    }
                }

                currentDay = currentDay.plusDays(1);
            }

            fillProgressThread.interrupt();
            try {
                fillProgressThread.join(1_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            final int filledFinal = totalFilled.get();
            final int failedFinal = totalFailed.get();
            printFillProgress(filledFinal + failedFinal, totalMissing, failedFinal, fillStart);
            PrettyPrint.clearProgress();

            closeAllAppenders(openAppenders);

            System.out.println();
            PrettyPrint.printBanner("PHASE 2 SUMMARY");
            System.out.printf("Missing blocks identified: %,d%n", totalMissing);
            System.out.printf("Blocks filled:            %,d%n", filledFinal);
            System.out.printf("Blocks failed:            %,d%n", failedFinal);
            if (failedFinal == 0 && filledFinal == totalMissing) {
                System.out.println(Ansi.AUTO.string("@|bold,green All missing blocks filled successfully.|@"));
            } else if (failedFinal > 0) {
                System.out.println(Ansi.AUTO.string("@|bold,red Some blocks could not be filled.|@ "
                        + "Run 'blocks validate' to check the current state."));
            }
            return failedFinal == 0 ? 0 : 1;

        } catch (Exception e) {
            System.err.println("Fatal error in fill phase: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────────────────────

    /**
     * Walk the output directory, open each zip, and identify those whose block range is fully
     * covered by the registry but whose entry count is below the expected maximum.
     *
     * @param outputDir       the wrapped-block root directory
     * @param registryHighest the highest block number stored in the block hash registry
     * @return sorted map from zip-range first block to {@link IncompleteZipInfo}
     * @throws IOException if directory walking fails
     */
    private static TreeMap<Long, IncompleteZipInfo> findIncompleteZips(final Path outputDir, final long registryHighest)
            throws IOException {
        final TreeMap<Long, IncompleteZipInfo> result = new TreeMap<>();
        final List<Path> allZips = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(outputDir)) {
            walk.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".zip"))
                    .sorted()
                    .forEach(allZips::add);
        }
        System.out.printf("  Found %,d zip files.%n", allZips.size());

        int checked = 0;
        for (final Path zipPath : allZips) {
            if (++checked % 1000 == 0) {
                System.out.printf("  Checked %,d / %,d …%n", checked, allZips.size());
            }

            final String name = zipPath.getFileName().toString();
            final int sIdx = name.indexOf('s');
            if (sIdx < 0) {
                continue;
            }
            final long rangeStart;
            try {
                rangeStart = Long.parseLong(name.substring(0, sIdx));
            } catch (NumberFormatException e) {
                continue;
            }

            if (rangeStart > registryHighest) {
                continue;
            }
            final long rangeEnd = Math.min(rangeStart + BLOCKS_PER_ZIP - 1, registryHighest);
            final long expectedCount = rangeEnd - rangeStart + 1;

            final Set<Long> present = new HashSet<>();
            try (FileSystem zipFs = FileSystems.newFileSystem(zipPath)) {
                try (Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
                    entries.filter(p -> p.getFileName().toString().endsWith(BLOCK_FILE_SUFFIX))
                            .forEach(p -> {
                                final String fn = p.getFileName().toString();
                                try {
                                    present.add(Long.parseLong(fn.substring(0, fn.indexOf('.'))));
                                } catch (NumberFormatException ignored) {
                                }
                            });
                }
            } catch (IOException e) {
                System.err.printf("  Warning: could not open zip %s: %s%n", zipPath, e.getMessage());
                continue;
            }

            if (present.size() < expectedCount) {
                result.put(rangeStart, new IncompleteZipInfo(zipPath, rangeStart, rangeEnd, present));
            }
        }
        return result;
    }

    /**
     * Close the {@link BlockZipAppender} for {@code zipFilePath} and remove it from the map.
     *
     * @param openAppenders map of currently open appenders
     * @param zipFilePath   the zip file whose appender to close
     */
    private static void closeAppender(final Map<Path, BlockZipAppender> openAppenders, final Path zipFilePath) {
        final BlockZipAppender appender = openAppenders.remove(zipFilePath);
        if (appender != null) {
            try {
                appender.close();
            } catch (IOException e) {
                System.err.printf("Warning: could not close zip %s: %s%n", zipFilePath, e.getMessage());
            }
        }
    }

    /**
     * Close all remaining open {@link BlockZipAppender}s.
     *
     * @param openAppenders the map of currently open appenders (will be empty after this call)
     */
    private static void closeAllAppenders(final Map<Path, BlockZipAppender> openAppenders) {
        for (final Map.Entry<Path, BlockZipAppender> e : new ArrayList<>(openAppenders.entrySet())) {
            try {
                e.getValue().close();
            } catch (IOException ex) {
                System.err.printf("Warning: could not close zip %s: %s%n", e.getKey(), ex.getMessage());
            }
        }
        openAppenders.clear();
    }

    /**
     * Print an in-place progress bar for the fill phase.
     *
     * @param done       number of blocks processed (filled + failed) so far
     * @param total      total missing blocks to fill
     * @param failed     number of blocks that failed to fill
     * @param startNanos {@link System#nanoTime()} when the fill phase started
     */
    private static void printFillProgress(final long done, final long total, final long failed, final long startNanos) {
        final double percent = total == 0 ? 100.0 : 100.0 * done / total;
        final long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        final long remainingMs = PrettyPrint.computeRemainingMilliseconds(done, total, elapsedMs);
        final String msg = String.format("Filling %,d / %,d blocks (%,d failed)", done, total, failed);
        PrettyPrint.printProgressWithEta(percent, msg, remainingMs);
    }

    // ── Data model ────────────────────────────────────────────────────────────────────────────────

    /**
     * Metadata for a single incomplete zip file discovered during the Phase 2 scan.
     *
     * @param zipPath       path to the zip file on disk
     * @param firstBlock    first block number in this zip's range (inclusive)
     * @param lastBlock     last block number in this zip's range (inclusive, capped at registry highest)
     * @param presentBlocks set of block numbers that already exist inside the zip
     */
    record IncompleteZipInfo(Path zipPath, long firstBlock, long lastBlock, Set<Long> presentBlocks) {}
}
