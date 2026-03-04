// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.blocks.AmendmentProvider.createAmendmentProvider;
import static org.hiero.block.tools.blocks.HasherStateFiles.loadWithFallback;
import static org.hiero.block.tools.blocks.model.BlockWriter.DEFAULT_COMPRESSION;
import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
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
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.DayBlockInfo;
import org.hiero.block.tools.records.model.parsed.ParsedRecordBlock;
import org.hiero.block.tools.records.model.parsed.RecordBlockConverter;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.PrettyPrint;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * CLI subcommand that repairs corrupt zip archives in a wrapped-block directory and, optionally,
 * fills any blocks that are still missing after repair.
 *
 * <h2>Phase 1 — CEN repair (always runs)</h2>
 *
 * <p>Scans all zip archives in the target directory, detects those with a corrupt or missing
 * central directory (CEN), and repairs them by rebuilding the CEN from the intact local file
 * entries read sequentially via {@link ZipInputStream}.</p>
 *
 * <h2>Why zips can be corrupt</h2>
 *
 * <p>A zip file's Central Directory is written at the very end when {@link ZipOutputStream#close()}
 * is called. If the {@code wrap} command was killed (SIGKILL, OOM, power loss) before
 * {@code close()} completed, the zip is left without a valid CEN. Additionally, the APPEND-mode
 * {@link ZipOutputStream} bug causes wrong CEN offsets whenever the stream's internal byte counter
 * starts at zero but the underlying file already contains data. The repair phase fixes both
 * problems.</p>
 *
 * <h2>Phase 2 — Fill missing blocks (runs when {@code -i} is provided)</h2>
 *
 * <p>After CEN repair, some zips may still be incomplete: blocks that were never flushed to disk
 * before the crash cannot be recovered from the zip itself. When the source record-file day
 * archives are available (supply their directory with {@code -i}), this phase re-wraps each
 * missing block from its original source file and appends it to the repaired zip.</p>
 *
 * <p>The block hash registry ({@code blockStreamBlockHashes.bin}) contains correct SHA-384 hashes
 * for every block that was ever processed. This command uses those hashes to reconstruct the exact
 * Merkle-tree state at each missing block position — so reconstruction is always possible without
 * reprocessing from block zero.</p>
 *
 * <h2>Performance design (Phase 1)</h2>
 *
 * <ul>
 *   <li><b>Scan</b> uses a two-seek check per file (~26 bytes read), parallelised across multiple
 *       threads to saturate USB drive throughput.</li>
 *   <li><b>Repair</b> is sequential and streams bytes directly without loading entire blocks into
 *       the heap.</li>
 * </ul>
 */
@SuppressWarnings("CallToPrintStackTrace")
@Command(
        name = "repair-zips",
        description =
                "Repair corrupt zip CENs in a wrapped-block directory, then optionally fill any blocks still missing"
                        + " from source day archives (-i)",
        mixinStandardHelpOptions = true)
public class RepairZipsCommand implements Callable<Integer> {

    @Parameters(index = "0", description = "Directory containing wrapped block zip files to scan and repair")
    private File directory;

    @Option(
            names = {"--scan-threads"},
            description = "Number of threads for the parallel CEN scan phase (default: 4). "
                    + "Increase for SSD-backed drives, keep low (1-2) for single-spindle HDDs.")
    private int scanThreads = 4;

    // ── Phase 2 (fill) options ────────────────────────────────────────────────────────────────────

    @Option(
            names = {"-i", "--input-dir"},
            description = "Directory containing source record-file .tar.zstd day archives. "
                    + "When provided, a second phase fills any blocks still missing after CEN repair. "
                    + "Omit to run Phase 1 (CEN repair) only.")
    private Path compressedDaysDir = null;

    @Option(
            names = {"-b", "--blocktimes-file"},
            description = "Block-times binary file for mapping block numbers to timestamps (default: block_times.bin)")
    private Path blockTimesFile = MetadataFiles.BLOCK_TIMES_FILE;

    @Option(
            names = {"-d", "--day-blocks"},
            description = "Day-blocks JSON file mapping calendar dates to first/last block numbers"
                    + " (default: day_blocks.json)")
    private Path dayBlocksFile = MetadataFiles.DAY_BLOCKS_FILE;

    @Option(
            names = {"-n", "--network"},
            description = "Network name for applying amendments (mainnet, testnet, none). Default: mainnet")
    private String network = "mainnet";

    @Option(
            names = {"--dry-run"},
            description = "Phase 2 only: scan and report missing blocks without re-wrapping or modifying any files")
    private boolean dryRun = false;

    // ── Constants ─────────────────────────────────────────────────────────────────────────────────

    /** EOCD (End of Central Directory) signature, little-endian. */
    private static final int EOCD_SIGNATURE = 0x06054b50;

    /** CEN (Central Directory Entry) signature, little-endian. */
    private static final int CEN_SIGNATURE = 0x02014b50;

    /** Minimum EOCD record size in bytes (no zip comment). */
    private static final int EOCD_SIZE = 22;

    /** Byte offset of the CEN offset field within the EOCD record. */
    private static final int EOCD_CEN_OFFSET_POS = 16;

    /** I/O buffer size for streaming repair operations (400 MiB). Should allow whole file to be read */
    private static final int BUFFER_SIZE = 1024 * 1024 * 400;

    /** Blocks per zip file — matches {@link BlockWriter#DEFAULT_POWERS_OF_TEN_PER_ZIP} = 4 (10,000 blocks). */
    private static final long BLOCKS_PER_ZIP = (long) Math.pow(10, BlockWriter.DEFAULT_POWERS_OF_TEN_PER_ZIP);

    /** File extension used for block entries inside zip files (e.g., {@code ".blk.zstd"}). */
    private static final String BLOCK_FILE_SUFFIX = ".blk" + DEFAULT_COMPRESSION.extension();

    /** Hex formatter for diagnostic hash output. */
    private static final HexFormat HEX = HexFormat.of();

    // ── Entry point ───────────────────────────────────────────────────────────────────────────────

    @Override
    public Integer call() {
        final Path dir = directory.toPath();
        if (!Files.isDirectory(dir)) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ Not a directory: " + dir));
            return 1;
        }

        // Phase 1: CEN repair — always runs
        final int repairExit = runRepairPhase(dir);

        if (compressedDaysDir == null) {
            System.out.println();
            System.out.println(Ansi.AUTO.string("@|yellow Tip:|@ Re-run with -i <source-days-dir> to also fill"
                    + " any blocks still missing after repair."));
            return repairExit;
        }

        // Phase 2: fill missing blocks from source day archives
        final int fillExit = runFillPhase(dir);
        return repairExit == 0 && fillExit == 0 ? 0 : 1;
    }

    // ── Phase 1: CEN repair ───────────────────────────────────────────────────────────────────────

    /**
     * Run the CEN repair phase: scan all zip files in {@code dir} for corrupt central directories
     * and rebuild them from the intact local file entries.
     *
     * @param dir the wrapped-block root directory
     * @return {@code 0} on success (all repaired), {@code 1} if any zip was unrecoverable
     */
    private int runRepairPhase(final Path dir) {
        PrettyPrint.printBanner("ZIP REPAIR — PHASE 1: CEN REPAIR");
        System.out.println(Ansi.AUTO.string("@|yellow Directory:|@ " + dir.toAbsolutePath()));
        System.out.println(Ansi.AUTO.string("@|yellow Scan threads:|@ " + scanThreads));
        System.out.println();

        // Collect all zip file paths
        System.out.print("Collecting zip file paths … ");
        System.out.flush();
        final List<Path> allZips = collectZipFiles(dir);
        System.out.println(allZips.size() + " found.");
        System.out.println();

        if (allZips.isEmpty()) {
            System.out.println(Ansi.AUTO.string("@|yellow No zip files found.|@"));
            return 0;
        }

        // Parallel fast-scan (~26 bytes read per file)
        System.out.println("Scanning " + allZips.size() + " zip files …");
        final long scanStart = System.nanoTime();
        final List<Path> corruptZips = findCorruptZipsParallel(allZips);
        final long scanSecs = (System.nanoTime() - scanStart) / 1_000_000_000L;
        System.out.printf(
                "  Done in %ds — %d corrupt file(s) out of %d.%n%n", scanSecs, corruptZips.size(), allZips.size());

        if (corruptZips.isEmpty()) {
            PrettyPrint.printBanner("PHASE 1 RESULT");
            System.out.println(Ansi.AUTO.string(
                    "@|bold,green All " + allZips.size() + " zip files have valid CENs. No repairs needed.|@"));
            return 0;
        }

        System.out.println(Ansi.AUTO.string("@|yellow Corrupt zip files:|@"));
        for (final Path corrupt : corruptZips) {
            System.out.println(Ansi.AUTO.string("  @|red •|@ " + corrupt));
        }
        System.out.println();

        // Sequential repair
        System.out.println("Repairing " + corruptZips.size() + " file(s) …");
        int repaired = 0;
        int unrecoverable = 0;
        for (final Path zipPath : corruptZips) {
            System.out.print(Ansi.AUTO.string("  @|yellow Repairing|@ " + zipPath + " … "));
            System.out.flush();
            final RepairResult result = repairZip(zipPath);
            switch (result.status()) {
                case REPAIRED -> {
                    repaired++;
                    System.out.println(
                            Ansi.AUTO.string("@|green OK|@ (" + result.entriesRecovered() + " blocks recovered)"));
                }
                case PARTIAL -> {
                    repaired++;
                    System.out.println(Ansi.AUTO.string("@|yellow PARTIAL|@ ("
                            + result.entriesRecovered()
                            + " blocks recovered; last entry was truncated: "
                            + result.detail() + ")"));
                }
                case UNRECOVERABLE -> {
                    unrecoverable++;
                    System.out.println(Ansi.AUTO.string("@|red FAILED|@ — " + result.detail()));
                }
            }
        }

        System.out.println();
        PrettyPrint.printBanner("PHASE 1 SUMMARY");
        System.out.println(Ansi.AUTO.string("@|yellow Zip files checked:|@   " + allZips.size()));
        System.out.println(Ansi.AUTO.string("@|yellow Corrupt found:|@        " + corruptZips.size()));
        System.out.println(Ansi.AUTO.string("@|yellow Successfully repaired:|@ " + repaired));
        System.out.println(Ansi.AUTO.string("@|yellow Unrecoverable:|@         " + unrecoverable));
        System.out.println();

        if (unrecoverable == 0) {
            System.out.println(Ansi.AUTO.string("@|bold,green All corrupt zip files have been repaired.|@"));
            return 0;
        } else {
            System.out.println(Ansi.AUTO.string("@|bold,red " + unrecoverable
                    + " zip file(s) could not be recovered.|@ "
                    + "The blocks they contained will be missing from validation."));
            return 1;
        }
    }

    // ── Phase 2: fill missing blocks ──────────────────────────────────────────────────────────────

    /**
     * Run the fill phase: scan for incomplete zip files (valid CEN but missing block entries) and
     * re-wrap each missing block from its original source record day archive.
     *
     * @param outputDir the wrapped-block root directory
     * @return {@code 0} on success, {@code 1} if any block could not be filled
     */
    private int runFillPhase(final Path outputDir) {
        // Validate prerequisites
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

        // Open block hash registry
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

            // Scan for incomplete zips
            System.out.println("\nScanning zip files for incomplete archives …");
            final long scanStart = System.nanoTime();
            final TreeMap<Long, IncompleteZipInfo> zipsByFirstBlock = findIncompleteZips(outputDir, registryHighest);
            final long scanSecs = (System.nanoTime() - scanStart) / 1_000_000_000L;
            System.out.printf("  Done in %ds — %d incomplete zip(s).%n%n", scanSecs, zipsByFirstBlock.size());

            if (zipsByFirstBlock.isEmpty()) {
                System.out.println(Ansi.AUTO.string("@|bold,green All zip files are complete. Nothing to fill.|@"));
                return 0;
            }

            // Print per-zip summary and count total missing blocks
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

            if (dryRun) {
                System.out.println(Ansi.AUTO.string("@|yellow DRY RUN — no changes made.|@"));
                return 0;
            }

            // Load metadata
            final Map<LocalDate, DayBlockInfo> dayMap = loadDayBlockInfoMap(dayBlocksFile);
            final Path addressBookFile = outputDir.resolve("addressBookHistory.json");
            final AddressBookRegistry addressBookRegistry = Files.exists(addressBookFile)
                    ? new AddressBookRegistry(addressBookFile)
                    : new AddressBookRegistry();
            final AmendmentProvider amendmentProvider = createAmendmentProvider(network);

            // Index available source day files by date
            final List<Path> allDayPaths = TarZstdDayUtils.sortedDayPaths(new File[] {compressedDaysDir.toFile()});
            final Map<LocalDate, Path> dateToDayPath = new HashMap<>();
            for (final Path p : allDayPaths) {
                dateToDayPath.put(
                        TarZstdDayUtils.parseDayFromFileName(p.getFileName().toString()), p);
            }

            // Determine the day range to process
            final long lowestFirstBlock = zipsByFirstBlock.firstKey();
            final long highestLastBlock =
                    zipsByFirstBlock.lastEntry().getValue().lastBlock();
            final LocalDate firstRelevantDay =
                    blockTimeReader.getBlockLocalDateTime(lowestFirstBlock).toLocalDate();
            final LocalDate lastRelevantDay =
                    blockTimeReader.getBlockLocalDateTime(highestLastBlock).toLocalDate();
            System.out.printf("Processing days: %s to %s%n%n", firstRelevantDay, lastRelevantDay);

            // Load streaming hasher and fast-replay to the first relevant day
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

            // Process each day in the relevant range
            int totalFilled = 0;
            int totalFailed = 0;
            final Map<Path, BlockZipAppender> openAppenders = new HashMap<>();

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

                // Check whether this day overlaps with any incomplete zip range
                final Map.Entry<Long, IncompleteZipInfo> floorEntry = zipsByFirstBlock.floorEntry(dayLast);
                final boolean hasIncompleteOverlap =
                        floorEntry != null && floorEntry.getValue().lastBlock() >= dayFirst;

                if (!hasIncompleteOverlap) {
                    // Fast-replay: this day has no missing blocks — update hasher from registry only
                    final long fromBlock = Math.max(hasherLeafCount, dayFirst);
                    for (long bn = fromBlock; bn <= dayLast; bn++) {
                        streamingHasher.addNodeByHash(blockRegistry.getBlockHash(bn));
                    }
                    hasherLeafCount = dayLast + 1;
                    currentDay = currentDay.plusDays(1);
                    continue;
                }

                // This day overlaps with at least one incomplete zip — read the source file
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
                                totalFailed++;
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

                System.out.printf(
                        "Processing day %s (%s) blocks %,d..%,d …%n",
                        currentDay, dayPath.getFileName(), dayFirst, dayLast);

                long blockNumInDay = dayFirst;
                try (Stream<UnparsedRecordBlock> dayStream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath)) {
                    // Cast stream::iterator as Iterable to use enhanced-for with break/continue without closing the
                    // stream.
                    for (final UnparsedRecordBlock unparsed : (Iterable<UnparsedRecordBlock>) dayStream::iterator) {
                        final long blockNum = blockNumInDay++;

                        // Catch up the hasher if it is behind (defensive)
                        while (hasherLeafCount < blockNum) {
                            streamingHasher.addNodeByHash(blockRegistry.getBlockHash(hasherLeafCount));
                            hasherLeafCount++;
                        }

                        // Determine whether this block is missing from its zip
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

                        // Re-wrap this missing block
                        final ParsedRecordBlock parsed = unparsed.parse();
                        final NodeAddressBook ab = addressBookRegistry.getAddressBookForBlock(parsed.blockTime());
                        final byte[] signedHash = parsed.recordFile().signedHash();
                        // this is parallel as signature validation is expensive CPU wise
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

                        // Verify the hash matches registry
                        final byte[] recomputedHash = hashBlock(wrapped);
                        final byte[] registryHash = blockRegistry.getBlockHash(blockNum);
                        if (!Arrays.equals(recomputedHash, registryHash)) {
                            System.err.printf(
                                    "ERROR: hash mismatch for block %,d!%n"
                                            + "  Registry: %s%n"
                                            + "  Computed: %s%n"
                                            + "  Skipping this block.%n",
                                    blockNum, HEX.formatHex(registryHash), HEX.formatHex(recomputedHash));
                            totalFailed++;
                            streamingHasher.addNodeByHash(registryHash);
                            hasherLeafCount = blockNum + 1;
                            continue;
                        }

                        // Append the re-wrapped block to the partial zip
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
                        totalFilled++;
                        System.out.printf("  Block %,d filled OK%n", blockNum);

                        // Close the appender once the zip's range is fully processed
                        final IncompleteZipInfo info = zipEntry.getValue();
                        if (blockNum == info.lastBlock()) {
                            closeAppender(openAppenders, zipFilePath);
                        }
                    }
                }

                currentDay = currentDay.plusDays(1);
            }

            closeAllAppenders(openAppenders);

            System.out.println();
            PrettyPrint.printBanner("PHASE 2 SUMMARY");
            System.out.printf("Missing blocks identified: %,d%n", totalMissing);
            System.out.printf("Blocks filled:            %,d%n", totalFilled);
            System.out.printf("Blocks failed:            %,d%n", totalFailed);
            if (totalFailed == 0 && totalFilled == totalMissing) {
                System.out.println(Ansi.AUTO.string("@|bold,green All missing blocks filled successfully.|@"));
            } else if (totalFailed > 0) {
                System.out.println(Ansi.AUTO.string("@|bold,red Some blocks could not be filled.|@ "
                        + "Run 'blocks validate' to check the current state."));
            }
            return totalFailed == 0 ? 0 : 1;

        } catch (Exception e) {
            System.err.println("Fatal error in fill phase: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }

    // ── Phase 1 helpers ───────────────────────────────────────────────────────────────────────────

    /**
     * Walk {@code dir} recursively and return all {@code .zip} file paths, sorted.
     *
     * @param dir root directory
     * @return sorted list of zip paths
     */
    private static List<Path> collectZipFiles(final Path dir) {
        final List<Path> result = new ArrayList<>();
        try {
            Files.walk(dir)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".zip"))
                    .sorted()
                    .forEach(result::add);
        } catch (IOException e) {
            System.err.println("Error scanning directory: " + e.getMessage());
        }
        return result;
    }

    /**
     * Scan all zips in parallel using a fixed thread pool.
     *
     * <p>Each file check is a fast two-seek operation reading only ~26 bytes.</p>
     *
     * @param allZips list of all zip paths to check
     * @return sorted the list of paths that failed the validity check
     */
    private List<Path> findCorruptZipsParallel(final List<Path> allZips) {
        final int total = allZips.size();
        final AtomicInteger checked = new AtomicInteger(0);
        final List<Path> corrupt = Collections.synchronizedList(new ArrayList<>());
        final long startNanos = System.nanoTime();

        try (ExecutorService pool = Executors.newFixedThreadPool(scanThreads)) {
            final List<Future<?>> futures = new ArrayList<>(total);
            for (final Path zip : allZips) {
                futures.add(pool.submit(() -> {
                    if (!isZipValid(zip)) {
                        corrupt.add(zip);
                    }
                    final int done = checked.incrementAndGet();
                    if (done % 1000 == 0 || done == total) {
                        final long elapsedSec = (System.nanoTime() - startNanos) / 1_000_000_000L;
                        System.out.printf(
                                "  Checked %,d / %,d  (%d corrupt so far)  [%ds]%n",
                                done, total, corrupt.size(), elapsedSec);
                    }
                }));
            }
            for (final Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    System.err.println("Scan task error: " + e.getMessage());
                }
            }
        }
        corrupt.sort(Path::compareTo);
        return corrupt;
    }

    /**
     * Fast zip validity check using two file-channel seeks, reading only ~26 bytes total.
     *
     * <p>A valid zip has an EOCD record at {@code fileSize - 22} with the correct signature, and
     * the first CEN entry at the declared offset also carries the correct signature. This catches
     * both a missing EOCD and an EOCD with wrong CEN offset (APPEND-mode bug).</p>
     *
     * @param zipPath path to the zip file to check
     * @return {@code true} if the zip appears valid
     */
    private static boolean isZipValid(final Path zipPath) {
        try {
            final long fileSize = Files.size(zipPath);
            if (fileSize < EOCD_SIZE) {
                return false;
            }
            try (SeekableByteChannel ch = Files.newByteChannel(zipPath)) {
                final ByteBuffer eocd = ByteBuffer.allocate(EOCD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
                ch.position(fileSize - EOCD_SIZE);
                if (ch.read(eocd) < EOCD_SIZE) {
                    return false;
                }
                eocd.flip();
                if (eocd.getInt(0) != EOCD_SIGNATURE) {
                    return false;
                }
                final long cenOffset = Integer.toUnsignedLong(eocd.getInt(EOCD_CEN_OFFSET_POS));
                if (cenOffset >= fileSize) {
                    return false;
                }
                final ByteBuffer cenSig = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
                ch.position(cenOffset);
                if (ch.read(cenSig) < 4) {
                    return false;
                }
                cenSig.flip();
                return cenSig.getInt(0) == CEN_SIGNATURE;
            }
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Repair a corrupt zip by streaming local entries from {@link ZipInputStream} into a new
     * {@link ZipOutputStream} written to a temp file, then atomically replacing the original.
     *
     * @param zipPath path to the corrupt zip file
     * @return result describing outcome
     */
    private static RepairResult repairZip(final Path zipPath) {
        final Path tempFile = zipPath.resolveSibling(zipPath.getFileName() + ".repairtmp");
        int entriesRecovered = 0;
        String truncationDetail = null;
        boolean streamOpenedOk = false;

        try (ZipInputStream zis =
                        new ZipInputStream(new BufferedInputStream(Files.newInputStream(zipPath), BUFFER_SIZE));
                ZipOutputStream zos =
                        new ZipOutputStream(new BufferedOutputStream(Files.newOutputStream(tempFile), BUFFER_SIZE))) {

            streamOpenedOk = true;
            zos.setMethod(ZipOutputStream.STORED);
            zos.setLevel(Deflater.NO_COMPRESSION);

            ZipEntry inEntry;
            while ((inEntry = zis.getNextEntry()) != null) {
                final String name = inEntry.getName();
                try {
                    writeEntry(zis, zos, inEntry);
                    entriesRecovered++;
                } catch (IOException e) {
                    truncationDetail = "entry '" + name + "' truncated: " + e.getMessage();
                    break;
                }
                zis.closeEntry();
            }

        } catch (IOException e) {
            cleanupTemp(tempFile);
            if (!streamOpenedOk || entriesRecovered == 0) {
                return RepairResult.unrecoverable(
                        "ZipInputStream failed before reading any entries: " + e.getMessage());
            }
            truncationDetail = "stream error after " + entriesRecovered + " entries: " + e.getMessage();
        }

        if (entriesRecovered == 0) {
            cleanupTemp(tempFile);
            return RepairResult.unrecoverable("No recoverable entries found in zip");
        }

        try {
            Files.move(tempFile, zipPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            cleanupTemp(tempFile);
            return RepairResult.unrecoverable("Failed to replace original with repaired file: " + e.getMessage());
        }

        return truncationDetail != null
                ? RepairResult.partial(entriesRecovered, truncationDetail)
                : RepairResult.repaired(entriesRecovered);
    }

    /**
     * Write one entry from {@code zis} to {@code zos}.
     *
     * <p>For STORED entries with size and CRC already set in the local header, uses streaming
     * transfer. For entries without pre-set metadata the bytes are buffered to compute the CRC.</p>
     *
     * @param zis     source stream, positioned at the start of the entry body
     * @param zos     destination stream
     * @param inEntry metadata from the source entry's local file header
     * @throws IOException if reading or writing fails
     */
    private static void writeEntry(final ZipInputStream zis, final ZipOutputStream zos, final ZipEntry inEntry)
            throws IOException {
        if (inEntry.getSize() >= 0 && inEntry.getCrc() != -1) {
            final ZipEntry outEntry = new ZipEntry(inEntry.getName());
            outEntry.setSize(inEntry.getSize());
            outEntry.setCompressedSize(inEntry.getCompressedSize());
            outEntry.setCrc(inEntry.getCrc());
            zos.putNextEntry(outEntry);
            zis.transferTo(zos);
            zos.closeEntry();
        } else {
            final byte[] bytes = zis.readAllBytes();
            final CRC32 crc = new CRC32();
            crc.update(bytes);
            final ZipEntry outEntry = new ZipEntry(inEntry.getName());
            outEntry.setSize(bytes.length);
            outEntry.setCompressedSize(bytes.length);
            outEntry.setCrc(crc.getValue());
            zos.putNextEntry(outEntry);
            zos.write(bytes);
            zos.closeEntry();
        }
    }

    private static void cleanupTemp(final Path tempFile) {
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException ignored) {
        }
    }

    // ── Phase 2 helpers ───────────────────────────────────────────────────────────────────────────

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
            // Zip file names follow the BlockWriter format: "{digit}{zeros}s.zip"
            // (e.g. "00000s.zip" for 10K blocks/zip). The 's' delimiter separates the
            // range-start digits from the trailing zeros — see BlockWriter.computeBlockPath.
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
     * @param openAppenders the map of currently open appenders
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

    // ── Result types (Phase 1) ────────────────────────────────────────────────────────────────────

    private enum RepairStatus {
        REPAIRED,
        PARTIAL,
        UNRECOVERABLE
    }

    private record RepairResult(RepairStatus status, int entriesRecovered, String detail) {
        static RepairResult repaired(final int count) {
            return new RepairResult(RepairStatus.REPAIRED, count, null);
        }

        static RepairResult partial(final int count, final String detail) {
            return new RepairResult(RepairStatus.PARTIAL, count, detail);
        }

        static RepairResult unrecoverable(final String detail) {
            return new RepairResult(RepairStatus.UNRECOVERABLE, 0, detail);
        }
    }

    // ── Data model (Phase 2) ──────────────────────────────────────────────────────────────────────

    /**
     * Metadata for a single incomplete zip file discovered during the Phase 2 scan.
     *
     * @param zipPath       path to the zip file on disk
     * @param firstBlock    first block number in this zip's range (inclusive)
     * @param lastBlock     last block number in this zip's range (inclusive, capped at registry highest)
     * @param presentBlocks set of block numbers that already exist inside the zip
     */
    private record IncompleteZipInfo(Path zipPath, long firstBlock, long lastBlock, Set<Long> presentBlocks) {}
}
