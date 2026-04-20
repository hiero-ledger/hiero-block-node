// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.days.subcommands;

import static org.hiero.block.tools.blocks.AmendmentProvider.createAmendmentProvider;
import static org.hiero.block.tools.blocks.HasherStateFiles.saveStateCheckpoint;
import static org.hiero.block.tools.blocks.model.BlockWriter.DEFAULT_COMPRESSION;
import static org.hiero.block.tools.blocks.model.BlockWriter.DEFAULT_POWERS_OF_TEN_PER_ZIP;
import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.findPrimaryRecord;
import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.findSidecars;
import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.findSignatures;
import static org.hiero.block.tools.utils.Md5Checker.checkMd5;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.pbj.runtime.Codec;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.AmendmentProvider;
import org.hiero.block.tools.blocks.HasherStateFiles;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockPath;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockZipAppender;
import org.hiero.block.tools.blocks.model.PreVerifiedBlock;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.blocks.validation.AddressBookUpdateValidation;
import org.hiero.block.tools.blocks.validation.BlockChainValidation;
import org.hiero.block.tools.blocks.validation.BlockStructureValidation;
import org.hiero.block.tools.blocks.validation.BlockValidation;
import org.hiero.block.tools.blocks.validation.HashRegistryValidation;
import org.hiero.block.tools.blocks.validation.HbarSupplyValidation;
import org.hiero.block.tools.blocks.validation.HistoricalBlockTreeValidation;
import org.hiero.block.tools.blocks.validation.JumpstartValidation;
import org.hiero.block.tools.blocks.validation.NodeStakeUpdateValidation;
import org.hiero.block.tools.blocks.validation.RequiredItemsValidation;
import org.hiero.block.tools.blocks.validation.SignatureBlockStats;
import org.hiero.block.tools.blocks.validation.SignatureStatsCollector;
import org.hiero.block.tools.blocks.validation.SignatureValidation;
import org.hiero.block.tools.blocks.validation.StreamingMerkleTreeValidation;
import org.hiero.block.tools.blocks.validation.TssEnablementValidation;
import org.hiero.block.tools.config.NetworkConfig;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.days.listing.DayListingFileReader;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.days.model.AddressBookRegistry;
import org.hiero.block.tools.days.model.NodeStakeRegistry;
import org.hiero.block.tools.days.model.TssEnablementRegistry;
import org.hiero.block.tools.metadata.MetadataFiles;
import org.hiero.block.tools.mirrornode.BlockInfo;
import org.hiero.block.tools.mirrornode.BlockTimeReader;
import org.hiero.block.tools.mirrornode.FetchBlockQuery;
import org.hiero.block.tools.mirrornode.FixBlockTime;
import org.hiero.block.tools.mirrornode.MirrorNodeBlockQueryOrder;
import org.hiero.block.tools.mirrornode.UpdateBlockData;
import org.hiero.block.tools.records.RecordFileUtils;
import org.hiero.block.tools.records.model.parsed.ParsedRecordBlock;
import org.hiero.block.tools.records.model.parsed.RecordBlockConverter;
import org.hiero.block.tools.records.model.parsed.ValidationException;
import org.hiero.block.tools.records.model.unparsed.InMemoryFile;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;
import org.hiero.block.tools.utils.ConcurrentTarZstdWriter;
import org.hiero.block.tools.utils.Gzip;
import org.hiero.block.tools.utils.gcp.ConcurrentDownloadManagerVirtualThreadsV3;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Live sequential block download with inline wrapping and full validation.
 *
 * <p>Downloads blocks one at a time, strictly sequentially, failing hard on any gap. Each block is:
 * <ol>
 *   <li>Downloaded from GCS</li>
 *   <li>Hash-chain validated and signature validated</li>
 *   <li>Written to per-day {@code .tar.zstd} archives</li>
 *   <li>Wrapped into block stream format (via {@link RecordBlockConverter})</li>
 *   <li>Validated against all 11 {@link BlockValidation} checks</li>
 *   <li>Written to hierarchical zip archives</li>
 * </ol>
 *
 * <p>This combines the functionality of {@code download-live2}, {@code wrap}, and {@code validate}
 * into a single pipeline optimized for correctness over throughput.
 */
@SuppressWarnings("FieldCanBeLocal")
@Command(
        name = "live-sequential",
        description = "Live sequential block download with inline validation and wrapping",
        mixinStandardHelpOptions = true)
public class LiveSequential implements Runnable {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Duration LIVE_POLL_INTERVAL = Duration.ofSeconds(2);
    private static final int PROGRESS_LOG_INTERVAL = 100;
    private static final long MIN_BLOCK_TIME_REFRESH_INTERVAL_MS = 60_000;
    private static final int WATERMARK_BATCH_SIZE = 256;

    /** How close to wall-clock time a block must be to count as "live edge". */
    private static final Duration LIVE_EDGE_THRESHOLD = Duration.ofMinutes(5);
    /** Maximum time to wait for all signatures at the live edge before proceeding with what we have. */
    private static final Duration MAX_SIG_WAIT = Duration.ofMinutes(2);

    /** Maximum number of 15-minute retries when waiting for GCS listings for a new day. */
    private static final int MAX_LISTING_WAIT_ATTEMPTS = 10;

    private static final File CACHE_DIR = new File("metadata/gcp-cache");

    /** Poison pill to signal the wrap+validate thread to exit. */
    private static final ValidatedBlock POISON_PILL = new ValidatedBlock(-1, null, List.of(), null, null);

    @Option(
            names = {"-l", "--listing-dir"},
            description = "Directory where listing files are stored (default: listingsByDay)")
    private File listingDir = new File("listingsByDay");

    @Option(
            names = {"-o", "--output-dir"},
            description = "Directory where compressed day archives are written (default: compressedDays)")
    private File outputDir = new File("compressedDays");

    @Option(
            names = {"--wrap-output-dir"},
            description = "Directory to write wrapped block stream output (default: wrappedBlocks)")
    private Path wrapOutputDir = Path.of("wrappedBlocks");

    @Option(
            names = {"--state-json"},
            description = "Path to state JSON file for resume (default: outputDir/validateCmdStatus.json)")
    private Path stateJsonPath;

    @Option(
            names = {"--address-book"},
            description = "Path to address book file for signature validation")
    private Path addressBookPath;

    @Option(
            names = {"--start-date"},
            description = "Start date in YYYY-MM-DD format (default: auto-detect from mirror node)")
    private String startDate;

    /** State persisted to JSON for resumability. Compatible with DownloadLive2 format. */
    private static class State {
        String dayDate;
        String recordFileTime;
        String endRunningHashHex;
        long blockNumber;

        State() {}

        State(long blockNumber, byte[] hash, Instant recordFileTime, LocalDate dayDate) {
            this.blockNumber = blockNumber;
            this.dayDate = dayDate != null ? dayDate.toString() : null;
            this.recordFileTime = recordFileTime != null ? recordFileTime.toString() : null;
            this.endRunningHashHex = hash != null ? HexFormat.of().formatHex(hash) : null;
        }

        byte[] getHashBytes() {
            return endRunningHashHex != null ? HexFormat.of().parseHex(endRunningHashHex) : null;
        }

        LocalDate getDayDate() {
            return dayDate != null ? LocalDate.parse(dayDate) : null;
        }
    }

    /** Block data passed from the download thread to the wrap+validate thread. */
    private record ValidatedBlock(
            long blockNumber,
            Instant recordFileTime,
            List<InMemoryFile> files,
            byte[] runningHash,
            LocalDate blockDay) {}

    /** A block whose file downloads have been fired but not yet joined. */
    private record PrefetchedBlock(
            long blockNumber,
            LocalDateTime blockTime,
            LocalDate blockDay,
            List<ListingRecordFile> orderedFiles,
            Set<ListingRecordFile> mostCommonFilesSet,
            List<CompletableFuture<InMemoryFile>> futures) {}

    @Override
    public void run() {
        System.out.println("[live-sequential] Starting sequential block download with inline wrapping + validation");
        System.out.println("Configuration:");
        System.out.println("  listingDir=" + listingDir);
        System.out.println("  outputDir=" + outputDir);
        System.out.println("  wrapOutputDir=" + wrapOutputDir);
        System.out.println("  stateJsonPath=" + stateJsonPath);
        System.out.println("  addressBookPath=" + addressBookPath);
        System.out.println("  startDate=" + (startDate != null ? startDate : "(auto-detect)"));

        try {
            // Create directories
            Files.createDirectories(outputDir.toPath());
            Files.createDirectories(wrapOutputDir);

            // Set default state file path
            if (stateJsonPath == null) {
                stateJsonPath = outputDir.toPath().resolve("validateCmdStatus.json");
            }
            if (stateJsonPath.getParent() != null) {
                Files.createDirectories(stateJsonPath.getParent());
            }

            // Initialize address book
            final AddressBookRegistry addressBookRegistry =
                    addressBookPath != null ? new AddressBookRegistry(addressBookPath) : new AddressBookRegistry();

            // Use HTTP transport for stability
            final Storage storage = StorageOptions.http()
                    .setProjectId(DownloadConstants.GCP_PROJECT_ID)
                    .build()
                    .getService();

            final ConcurrentDownloadManagerVirtualThreadsV3 downloadManager =
                    ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(storage)
                            .setMaxConcurrency(64)
                            .build();

            final BlockTimeReader blockTimeReader = new BlockTimeReader();

            // Determine starting point
            final State initialState = determineStartingPoint(blockTimeReader);
            System.out.println("[live-sequential] Starting from block " + initialState.blockNumber
                    + " hash["
                    + (initialState.endRunningHashHex != null
                            ? initialState.endRunningHashHex.substring(
                                    0, Math.min(8, initialState.endRunningHashHex.length()))
                            : "null")
                    + "]"
                    + " day=" + initialState.dayDate);

            // Create producer-consumer queue
            final BlockingQueue<ValidatedBlock> queue = new LinkedBlockingQueue<>(32);
            final AtomicReference<Throwable> wrapError = new AtomicReference<>(null);

            // Start wrap+validate thread
            final Thread wrapThread = new Thread(
                    () -> {
                        try {
                            runWrapAndValidateThread(queue, addressBookRegistry);
                        } catch (Throwable t) {
                            wrapError.set(t);
                            System.err.println("[WRAP] Fatal error in wrap+validate thread: " + t.getMessage());
                            t.printStackTrace();
                        }
                    },
                    "wrap-validate-thread");
            wrapThread.setDaemon(true);
            wrapThread.start();

            // Add shutdown hook
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(
                            () -> {
                                System.out.println("[live-sequential] Shutdown requested...");
                                downloadManager.close();
                            },
                            "live-sequential-shutdown"));

            // Main download loop
            processBlocksSequentially(
                    initialState, downloadManager, blockTimeReader, queue, wrapError, addressBookRegistry);

            // Signal wrap thread to exit and wait
            queue.put(POISON_PILL);
            wrapThread.join(60_000);

            // Check for wrap errors
            Throwable wrapErr = wrapError.get();
            if (wrapErr != null) {
                throw new RuntimeException("Wrap+validate thread failed", wrapErr);
            }

            System.out.println("[live-sequential] Complete.");
        } catch (Exception e) {
            System.err.println("[live-sequential] Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Determines the starting point for block processing.
     * Priority: 1) Wrap effective highest (accounts for mid-zip truncation), 2) Resume from state file,
     * 3) Use --start-date, 4) Auto-detect from mirror node
     */
    private State determineStartingPoint(BlockTimeReader blockTimeReader) {
        // Priority 1: Resume from wrap state (the authoritative source, accounts for mid-zip truncation)
        long wrapEffective = computeWrapEffectiveHighest();
        if (wrapEffective >= 0) {
            System.out.println("[live-sequential] Resuming from wrap effective highest: block " + wrapEffective);
            // If the day's tar archive doesn't exist, back up to the start of that day
            // so the tar gets all blocks for the day (wrap thread skips already-wrapped blocks)
            try {
                LocalDateTime blockTime = blockTimeReader.getBlockLocalDateTime(wrapEffective);
                LocalDate day = blockTime.toLocalDate();
                Path dayArchive = outputDir.toPath().resolve(day + ".tar.zstd");
                if (!Files.exists(dayArchive)) {
                    LocalDateTime startOfDay = day.atStartOfDay();
                    long firstBlockOfDay = blockTimeReader.getNearestBlockAfterTime(startOfDay);
                    long dlStart = firstBlockOfDay - 1;
                    System.out.println("[live-sequential] No tar archive for " + day
                            + ", backing up download start to block " + dlStart
                            + " (start of day) for complete tar");
                    State state = new State();
                    state.blockNumber = dlStart;
                    return state;
                }
            } catch (Exception e) {
                System.err.println("[live-sequential] Warning: could not check day archive: " + e.getMessage());
            }
            State state = new State();
            state.blockNumber = wrapEffective;
            return state;
        }

        // Priority 2: Resume from state file
        if (Files.exists(stateJsonPath)) {
            try {
                String json = Files.readString(stateJsonPath, StandardCharsets.UTF_8);
                State state = GSON.fromJson(json, State.class);
                if (state != null && state.blockNumber > 0) {
                    System.out.println("[live-sequential] Resuming from state file: block " + state.blockNumber);
                    return state;
                }
            } catch (Exception e) {
                System.err.println("[live-sequential] Warning: Failed to read state file: " + e.getMessage());
            }
        }

        // Priority 3: Use --start-date
        if (startDate != null && !startDate.isBlank()) {
            LocalDate targetDay = LocalDate.parse(startDate);
            System.out.println("[live-sequential] Using provided start date: " + targetDay);

            LocalDateTime startOfDay = targetDay.atStartOfDay();
            long firstBlockOfDay = blockTimeReader.getNearestBlockAfterTime(startOfDay);

            System.out.println("[live-sequential] First block of " + targetDay + " is " + firstBlockOfDay);

            State state = new State();
            state.blockNumber = firstBlockOfDay - 1;
            state.dayDate = targetDay.toString();
            return state;
        }

        // Priority 4: Auto-detect from mirror node
        System.out.println("[live-sequential] Querying mirror node for current day...");
        List<BlockInfo> latestBlocks = FetchBlockQuery.getLatestBlocks(1, MirrorNodeBlockQueryOrder.DESC);

        if (latestBlocks.isEmpty()) {
            throw new RuntimeException("Failed to get latest block from mirror node");
        }

        BlockInfo latestBlock = latestBlocks.getFirst();
        String timestamp = latestBlock.timestampFrom != null ? latestBlock.timestampFrom : latestBlock.timestampTo;

        if (timestamp == null) {
            throw new RuntimeException("Latest block has no timestamp");
        }

        String[] parts = timestamp.split("\\.");
        long epochSeconds = Long.parseLong(parts[0]);
        Instant blockInstant = Instant.ofEpochSecond(epochSeconds);
        LocalDate today = blockInstant.atZone(ZoneOffset.UTC).toLocalDate();

        System.out.println("[live-sequential] Detected current day: " + today);

        LocalDateTime startOfDay = today.atStartOfDay();
        long firstBlockOfDay = blockTimeReader.getNearestBlockAfterTime(startOfDay);

        State state = new State();
        state.blockNumber = firstBlockOfDay - 1;
        state.dayDate = today.toString();
        return state;
    }

    /**
     * Computes the wrap thread's effective highest block, including mid-zip truncation.
     * This mirrors the logic in {@link #runWrapAndValidateThread} so the download thread
     * starts from the same block the wrap thread expects.
     *
     * @return the effective highest block, or -1 if no wrap state exists
     */
    private long computeWrapEffectiveHighest() {
        final Path hashRegistryPath = wrapOutputDir.resolve("blockStreamBlockHashes.bin");
        final Path watermarkFile = wrapOutputDir.resolve("wrap-commit.bin");

        if (!Files.exists(hashRegistryPath)) {
            return -1;
        }

        try (BlockStreamBlockHashRegistry blockRegistry = new BlockStreamBlockHashRegistry(hashRegistryPath)) {
            long watermark = loadWatermark(watermarkFile);
            long registryHighest = blockRegistry.highestBlockNumberStored();

            if (watermark >= 0 && registryHighest > watermark) {
                registryHighest = watermark;
            }

            // Mid-zip truncation (same logic as runWrapAndValidateThread)
            if (registryHighest >= 0) {
                long zipRangeFirst = BlockWriter.zipRangeFirstBlock(registryHighest, DEFAULT_POWERS_OF_TEN_PER_ZIP);
                long blocksPerZip = (long) Math.pow(10, DEFAULT_POWERS_OF_TEN_PER_ZIP);
                long zipRangeLast = zipRangeFirst + blocksPerZip - 1;
                if (registryHighest < zipRangeLast) {
                    registryHighest = zipRangeFirst - 1;
                }
            }

            return registryHighest;
        } catch (Exception e) {
            System.err.println("[live-sequential] Warning: Failed to read wrap state: " + e.getMessage());
            return -1;
        }
    }

    /**
     * Main sequential download loop. Downloads one block at a time, validates, writes to tar.zstd,
     * and queues for wrapping.
     */
    private void processBlocksSequentially(
            State initialState,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader initialBlockTimeReader,
            BlockingQueue<ValidatedBlock> queue,
            AtomicReference<Throwable> wrapError,
            AddressBookRegistry addressBookRegistry)
            throws Exception {

        long currentBlockNumber = initialState.blockNumber;
        byte[] currentHash = initialState.getHashBytes();
        LocalDate currentDay = initialState.getDayDate();
        BlockTimeReader blockTimeReader = initialBlockTimeReader;

        long blocksProcessedTotal = 0;
        long blocksProcessedToday = 0;
        long dayStartTime = System.currentTimeMillis();
        long lastBlockTimeRefreshMs = 0;

        // Cache for listing files and grouped map - reload only when day changes
        List<ListingRecordFile> cachedListingFiles = null;
        Map<LocalDateTime, List<ListingRecordFile>> cachedFilesByBlock = null;
        LocalDate cachedListingDay = null;

        // Sliding window: prefetch up to PREFETCH_WINDOW blocks ahead
        final int PREFETCH_WINDOW = 8;
        final ArrayDeque<PrefetchedBlock> prefetchWindow = new ArrayDeque<>();
        long nextPrefetchBlock = currentBlockNumber + 1; // next block number to fire downloads for

        final NetworkConfig netConfig = NetworkConfig.current();

        try {
            while (true) {
                // Check for wrap thread errors
                Throwable wrapErr = wrapError.get();
                if (wrapErr != null) {
                    throw new RuntimeException("Wrap+validate thread failed, stopping download", wrapErr);
                }

                long nextBlockNumber = currentBlockNumber + 1;

                // Step 1: Get block timestamp
                LocalDateTime blockTime;
                try {
                    blockTime = blockTimeReader.getBlockLocalDateTime(nextBlockNumber);
                } catch (Exception e) {
                    long now = System.currentTimeMillis();
                    if (now - lastBlockTimeRefreshMs >= MIN_BLOCK_TIME_REFRESH_INTERVAL_MS) {
                        System.out.println(
                                "[LIVE] Block " + nextBlockNumber + " not in BlockTimeReader, refreshing...");
                        UpdateBlockData.updateBlockTimesOnly(MetadataFiles.BLOCK_TIMES_FILE);
                        blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
                        lastBlockTimeRefreshMs = now;
                    }
                    // Drain the prefetch window — we can't resolve more blocks yet
                    prefetchWindow.clear();
                    nextPrefetchBlock = nextBlockNumber;
                    Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                    continue;
                }

                LocalDate blockDay = blockTime.toLocalDate();

                // Step 2: Handle day boundary
                if (!blockDay.equals(currentDay)) {
                    if (currentDay != null) {
                        System.out.println("[live-sequential] Day completed: " + currentDay + " ("
                                + blocksProcessedToday + " blocks in "
                                + formatDuration((System.currentTimeMillis() - dayStartTime) / 1000) + ")");
                    }

                    // Start new day — clear prefetch since listings change
                    currentDay = blockDay;
                    blocksProcessedToday = 0;
                    dayStartTime = System.currentTimeMillis();
                    cachedListingFiles = null;
                    cachedFilesByBlock = null;
                    cachedListingDay = null;
                    prefetchWindow.clear();
                    nextPrefetchBlock = nextBlockNumber;

                    System.out.println("[live-sequential] Started new day: " + currentDay);
                }

                // Step 3: Load GCS listings if day changed
                if (!blockDay.equals(cachedListingDay)) {
                    int year = blockTime.getYear();
                    int month = blockTime.getMonthValue();
                    int day = blockTime.getDayOfMonth();

                    int attempt = 0;
                    while (true) {
                        attempt++;
                        refreshListingsForDay(blockDay, netConfig);
                        try {
                            cachedListingFiles =
                                    DayListingFileReader.loadRecordsFileForDay(listingDir.toPath(), year, month, day);
                            cachedFilesByBlock = cachedListingFiles.stream()
                                    .collect(Collectors.groupingBy(ListingRecordFile::timestamp));
                            cachedListingDay = blockDay;
                            System.out.println("[live-sequential] Loaded " + cachedListingFiles.size()
                                    + " listing entries for " + blockDay);
                            break;
                        } catch (NoSuchFileException e) {
                            if (attempt >= MAX_LISTING_WAIT_ATTEMPTS) {
                                throw new IllegalStateException("Listings not available for " + blockDay + " after "
                                        + attempt + " attempts (waited " + (attempt * 15) + " minutes)");
                            }
                            System.out.println("[live-sequential] Listings not available yet for " + blockDay
                                    + ", waiting 15 minutes... (attempt " + attempt + "/"
                                    + MAX_LISTING_WAIT_ATTEMPTS + ")");
                            Thread.sleep(15 * 60 * 1000);
                        }
                    }
                }

                // Step 4: Fill the prefetch window — fire downloads for blocks ahead
                while (prefetchWindow.size() < PREFETCH_WINDOW) {
                    long target = nextPrefetchBlock;
                    if (target < nextBlockNumber) {
                        target = nextBlockNumber;
                        nextPrefetchBlock = nextBlockNumber;
                    }
                    try {
                        LocalDateTime targetTime = blockTimeReader.getBlockLocalDateTime(target);
                        LocalDate targetDay = targetTime.toLocalDate();
                        // Stop prefetching across day boundary (listings may differ)
                        if (!targetDay.equals(cachedListingDay)) {
                            break;
                        }
                        List<ListingRecordFile> group = cachedFilesByBlock.get(targetTime);
                        if (group == null || group.isEmpty()) {
                            break; // block not in listings yet
                        }
                        // Don't prefetch if no primary record file in listings (incomplete upload)
                        boolean hasRecord = group.stream().anyMatch(f -> f.type() == ListingRecordFile.Type.RECORD);
                        long sigFiles = group.stream()
                                .filter(f -> f.type() == ListingRecordFile.Type.RECORD_SIG)
                                .count();
                        if (!hasRecord || sigFiles < 3) {
                            break; // incomplete upload — stop prefetching
                        }
                        List<ListingRecordFile> ordered = resolveOrderedFiles(group);
                        Set<ListingRecordFile> common = resolveMostCommonFilesSet(group);
                        List<CompletableFuture<InMemoryFile>> futures =
                                fireDownloads(ordered, netConfig, downloadManager);
                        prefetchWindow.addLast(
                                new PrefetchedBlock(target, targetTime, targetDay, ordered, common, futures));
                        nextPrefetchBlock = target + 1;
                    } catch (Exception e) {
                        break; // block time not available — stop filling
                    }
                }

                // Step 5: Get downloads for current block (from window or fresh)
                List<CompletableFuture<InMemoryFile>> downloadFutures;
                List<ListingRecordFile> orderedFiles;
                Set<ListingRecordFile> mostCommonFilesSet;

                PrefetchedBlock head = prefetchWindow.peekFirst();
                if (head != null && head.blockNumber == nextBlockNumber) {
                    // Use prefetched downloads
                    prefetchWindow.pollFirst();
                    downloadFutures = head.futures;
                    orderedFiles = head.orderedFiles;
                    mostCommonFilesSet = head.mostCommonFilesSet;
                } else {
                    // Prefetch miss — resolve and download normally
                    // Clear stale window entries
                    prefetchWindow.clear();
                    nextPrefetchBlock = nextBlockNumber + 1;

                    List<ListingRecordFile> group = cachedFilesByBlock.get(blockTime);
                    if (group == null || group.isEmpty()) {
                        refreshListingsForSingleDay(blockDay, netConfig);
                        cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                                listingDir.toPath(),
                                blockTime.getYear(),
                                blockTime.getMonthValue(),
                                blockTime.getDayOfMonth());
                        cachedFilesByBlock = cachedListingFiles.stream()
                                .collect(Collectors.groupingBy(ListingRecordFile::timestamp));
                        cachedListingDay = blockDay;
                        group = cachedFilesByBlock.get(blockTime);

                        if (group == null || group.isEmpty()) {
                            System.out.println("[live-sequential] No files found for block " + nextBlockNumber
                                    + " at time " + blockTime + ", fixing block times...");
                            FixBlockTime.fixBlockTimeRange(
                                    MetadataFiles.BLOCK_TIMES_FILE, nextBlockNumber, nextBlockNumber + 100);
                            blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
                            continue;
                        }
                    }

                    orderedFiles = resolveOrderedFiles(group);
                    mostCommonFilesSet = resolveMostCommonFilesSet(group);
                    downloadFutures = fireDownloads(orderedFiles, netConfig, downloadManager);
                }

                // If no hash yet, fetch from mirror node
                if (currentHash == null && nextBlockNumber > 0) {
                    System.out.println(
                            "[live-sequential] Fetching previous hash from mirror node for block " + nextBlockNumber);
                    try {
                        var prevHashBytes = FetchBlockQuery.getPreviousHashForBlock(nextBlockNumber);
                        currentHash = prevHashBytes.toByteArray();
                        System.out.println("[live-sequential] Got previous hash: "
                                + HexFormat.of().formatHex(currentHash).substring(0, 16) + "...");
                    } catch (Exception e) {
                        System.err.println(
                                "[live-sequential] Warning: Could not fetch previous hash: " + e.getMessage());
                    }
                }

                // Step 6: Process download results
                List<InMemoryFile> inMemoryFiles = new ArrayList<>();
                for (int fi = 0; fi < orderedFiles.size(); fi++) {
                    ListingRecordFile lr = orderedFiles.get(fi);
                    String blobName = netConfig.bucketPathPrefix() + lr.path();
                    try {
                        InMemoryFile downloadedFile = downloadFutures.get(fi).join();

                        String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);

                        boolean md5Valid = checkMd5(lr.md5Hex(), downloadedFile.data());
                        if (!md5Valid) {
                            System.err.println("[live-sequential] MD5 mismatch for " + lr.path() + ", skipping");
                            continue;
                        }

                        byte[] contentBytes = downloadedFile.data();
                        if (filename.endsWith(".gz")) {
                            contentBytes = Gzip.ungzipInMemory(contentBytes);
                            filename = filename.replaceAll("\\.gz$", "");
                        }

                        Path newFilePath = DownloadDayLiveImpl.computeNewFilePath(lr, mostCommonFilesSet, filename);
                        inMemoryFiles.add(new InMemoryFile(newFilePath, contentBytes));
                    } catch (CompletionException ce) {
                        System.err.println(
                                "[live-sequential] Download failed for " + blobName + ": " + ce.getMessage());
                        throw new IllegalStateException("Download failed for block " + nextBlockNumber, ce.getCause());
                    }
                }

                // Ensure primary record file is first (validateBlockHashes assumes getFirst() is the record)
                inMemoryFiles.sort((a, b) -> {
                    String aName = a.path().getFileName().toString();
                    String bName = b.path().getFileName().toString();
                    boolean aIsRecord = aName.endsWith(".rcd") && !aName.contains("_sig") && !aName.contains("_node_");
                    boolean bIsRecord = bName.endsWith(".rcd") && !bName.contains("_sig") && !bName.contains("_node_");
                    return Boolean.compare(bIsRecord, aIsRecord);
                });

                // Verify primary record file exists — if not, listings may be incomplete (near live tip)
                if (inMemoryFiles.isEmpty()) {
                    System.out.println("[live-sequential] No files downloaded for block " + nextBlockNumber
                            + ", waiting for GCS uploads...");
                    prefetchWindow.clear();
                    nextPrefetchBlock = nextBlockNumber;
                    Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                    continue;
                }
                String firstFileName =
                        inMemoryFiles.getFirst().path().getFileName().toString();
                if (!firstFileName.endsWith(".rcd") || firstFileName.contains("_sig")) {
                    System.out.println("[live-sequential] No primary record file for block " + nextBlockNumber
                            + " (first file: " + firstFileName + "), refreshing listings...");
                    refreshListingsForSingleDay(blockDay, netConfig);
                    cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                            listingDir.toPath(),
                            blockTime.getYear(),
                            blockTime.getMonthValue(),
                            blockTime.getDayOfMonth());
                    cachedFilesByBlock =
                            cachedListingFiles.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));
                    cachedListingDay = blockDay;
                    prefetchWindow.clear();
                    nextPrefetchBlock = nextBlockNumber;
                    Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                    continue;
                }

                // Verify sufficient signature files (need at least 3/N for consensus)
                long sigCount = inMemoryFiles.stream()
                        .filter(f -> f.path().getFileName().toString().contains("_sig"))
                        .count();
                long maxExpectedSigs = addressBookRegistry
                        .getCurrentAddressBook()
                        .nodeAddress()
                        .size();
                if (sigCount < 3) {
                    System.out.println("[live-sequential] Insufficient signatures for block " + nextBlockNumber + " ("
                            + sigCount + "/" + maxExpectedSigs + "), waiting for GCS uploads...");
                    refreshListingsForSingleDay(blockDay, netConfig);
                    cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                            listingDir.toPath(),
                            blockTime.getYear(),
                            blockTime.getMonthValue(),
                            blockTime.getDayOfMonth());
                    cachedFilesByBlock =
                            cachedListingFiles.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));
                    cachedListingDay = blockDay;
                    prefetchWindow.clear();
                    nextPrefetchBlock = nextBlockNumber;
                    Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                    continue;
                }

                // At the live edge, wait for all signatures before proceeding.
                // This ensures the tar archive has complete signature coverage.
                Instant blockInstantUtc = blockTime.atZone(ZoneOffset.UTC).toInstant();
                boolean isLiveEdge =
                        Duration.between(blockInstantUtc, Instant.now()).compareTo(LIVE_EDGE_THRESHOLD) < 0;
                if (isLiveEdge) {
                    // Count expected signatures from GCS listing
                    List<ListingRecordFile> currentGroup = cachedFilesByBlock.get(blockTime);
                    long expectedSigs = currentGroup != null
                            ? currentGroup.stream()
                                    .filter(f -> f.type() == ListingRecordFile.Type.RECORD_SIG)
                                    .count()
                            : 0;
                    if (sigCount < expectedSigs) {
                        // Some listed sigs failed MD5 — re-download; treat like insufficient
                        System.out.println("[live-sequential] Block " + nextBlockNumber + " has " + sigCount + "/"
                                + expectedSigs + " sigs (some failed MD5), retrying...");
                        prefetchWindow.clear();
                        nextPrefetchBlock = nextBlockNumber;
                        Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                        continue;
                    }
                    // Check if more sigs might still be uploading to GCS
                    // (reuse maxExpectedSigs computed above)

                    if (expectedSigs < maxExpectedSigs) {
                        long waitedMs =
                                Duration.between(blockInstantUtc, Instant.now()).toMillis();
                        if (waitedMs < MAX_SIG_WAIT.toMillis()) {
                            System.out.println("[live-sequential] Block " + nextBlockNumber + " has " + sigCount + "/"
                                    + maxExpectedSigs + " sigs, waiting for remaining ("
                                    + (MAX_SIG_WAIT.toMillis() - waitedMs) / 1000 + "s left)...");
                            refreshListingsForSingleDay(blockDay, netConfig);
                            cachedListingFiles = DayListingFileReader.loadRecordsFileForDay(
                                    listingDir.toPath(),
                                    blockTime.getYear(),
                                    blockTime.getMonthValue(),
                                    blockTime.getDayOfMonth());
                            cachedFilesByBlock = cachedListingFiles.stream()
                                    .collect(Collectors.groupingBy(ListingRecordFile::timestamp));
                            cachedListingDay = blockDay;
                            prefetchWindow.clear();
                            nextPrefetchBlock = nextBlockNumber;
                            Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
                            continue;
                        }
                        System.out.println("[live-sequential] Block " + nextBlockNumber
                                + " timed out waiting for all sigs, proceeding with " + sigCount + "/"
                                + maxExpectedSigs);
                    } else {
                        System.out.println("[live-sequential] Block " + nextBlockNumber + " has all signatures ("
                                + sigCount + "/" + maxExpectedSigs + ")");
                    }
                }

                // Step 7: Validate hash chain (lightweight, keeps sequential integrity)
                // TODO: download-live2 also calls UnparsedRecordBlockV6.validate() which verifies sidecar
                // SHA-384 hashes against the record file metadata. This pipeline only does MD5 at download
                // time + RSA verification in the wrap thread. Consider adding sidecar hash verification.
                byte[] newHash =
                        DownloadDayLiveImpl.validateBlockHashes(nextBlockNumber, inMemoryFiles, currentHash, null);
                Instant recordFileTime = blockTime.atZone(ZoneOffset.UTC).toInstant();

                // Step 8: Assert sequential ordering
                if (nextBlockNumber != currentBlockNumber + 1) {
                    throw new IllegalStateException(
                            "Block gap detected: expected " + (currentBlockNumber + 1) + " but got " + nextBlockNumber);
                }

                // Step 9: Queue for wrapping (tar writing moved to wrap thread)
                queue.put(new ValidatedBlock(nextBlockNumber, recordFileTime, inMemoryFiles, newHash, blockDay));

                // Step 10: Update state
                currentBlockNumber = nextBlockNumber;
                currentHash = newHash;
                blocksProcessedTotal++;
                blocksProcessedToday++;

                // Save state periodically (every 10 blocks) to reduce disk I/O
                if (blocksProcessedTotal % 10 == 0) {
                    saveState(new State(currentBlockNumber, currentHash, recordFileTime, currentDay));
                }

                // Progress logging
                if (blocksProcessedTotal % PROGRESS_LOG_INTERVAL == 0) {
                    long elapsed = System.currentTimeMillis() - dayStartTime;
                    double blocksPerSec = blocksProcessedToday / Math.max(1.0, elapsed / 1000.0);
                    System.out.println("[live-sequential] Block " + currentBlockNumber + " (" + blocksProcessedToday
                            + " today, " + String.format("%.1f", blocksPerSec) + " blocks/sec, queue="
                            + queue.size() + "/" + 32 + ")");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[live-sequential] Interrupted, saving state...");
        } finally {
            if (currentHash != null) {
                try {
                    LocalDateTime finalBlockTime = blockTimeReader.getBlockLocalDateTime(currentBlockNumber);
                    Instant finalRecordTime =
                            finalBlockTime.atZone(ZoneOffset.UTC).toInstant();
                    saveState(new State(currentBlockNumber, currentHash, finalRecordTime, currentDay));
                    System.out.println("[live-sequential] Saved state at block " + currentBlockNumber);
                } catch (Exception e) {
                    System.err.println("[live-sequential] Error saving final state: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Wrap+validate consumer thread. Takes downloaded blocks from the queue, wraps them into
     * block stream format, and runs all 11 BlockValidation checks.
     */
    private void runWrapAndValidateThread(BlockingQueue<ValidatedBlock> queue, AddressBookRegistry addressBookRegistry)
            throws Exception {

        // Initialize wrapping state
        final Path hashRegistryPath = wrapOutputDir.resolve("blockStreamBlockHashes.bin");
        final Path streamingMerkleTreeFile = wrapOutputDir.resolve("streamingMerkleTree.bin");
        final Path watermarkFile = wrapOutputDir.resolve("wrap-commit.bin");
        final Path addressBookFile = wrapOutputDir.resolve("addressBookHistory.json");
        final Path checkpointDir = wrapOutputDir.resolve("validateCheckpoint");

        Files.createDirectories(wrapOutputDir);
        Files.createDirectories(checkpointDir);

        // Copy address book to wrap output if needed
        if (!Files.exists(addressBookFile) && addressBookPath != null && Files.exists(addressBookPath)) {
            Files.copy(addressBookPath, addressBookFile);
        }

        final AmendmentProvider amendmentProvider =
                createAmendmentProvider(NetworkConfig.current().networkName());

        try (BlockStreamBlockHashRegistry blockRegistry = new BlockStreamBlockHashRegistry(hashRegistryPath)) {

            // Load watermark and reconcile
            long watermark = loadWatermark(watermarkFile);
            long registryHighest = blockRegistry.highestBlockNumberStored();

            if (watermark >= 0 && registryHighest > watermark) {
                System.out.println(
                        "[WRAP] Registry at " + registryHighest + " but watermark at " + watermark + "; truncating.");
                blockRegistry.truncateTo(watermark);
            } else if (watermark < 0 && registryHighest >= 0) {
                watermark = registryHighest;
            }

            long effectiveHighest = blockRegistry.highestBlockNumberStored();

            // Mid-zip resume: back up to zip range start if needed
            if (effectiveHighest >= 0) {
                long zipRangeFirst = BlockWriter.zipRangeFirstBlock(effectiveHighest, DEFAULT_POWERS_OF_TEN_PER_ZIP);
                long blocksPerZip = (long) Math.pow(10, DEFAULT_POWERS_OF_TEN_PER_ZIP);
                long zipRangeLast = zipRangeFirst + blocksPerZip - 1;
                if (effectiveHighest < zipRangeLast) {
                    long truncateTo = zipRangeFirst - 1;
                    System.out.println("[WRAP] Mid-zip resume: truncating to " + truncateTo);
                    BlockPath partialZipPath = BlockWriter.computeBlockPath(
                            wrapOutputDir, effectiveHighest, BlockArchiveType.UNCOMPRESSED_ZIP);
                    Files.deleteIfExists(partialZipPath.zipFilePath());
                    blockRegistry.truncateTo(truncateTo);
                    saveWatermark(watermarkFile, truncateTo);
                    effectiveHighest = blockRegistry.highestBlockNumberStored();
                }
            }

            System.out.println("[WRAP] Starting from block: " + effectiveHighest);

            // Create streaming hasher and replay from registry
            final StreamingHasher streamingHasher = new StreamingHasher();
            if (effectiveHighest >= 0) {
                System.out.println("[WRAP] Replaying " + (effectiveHighest + 1) + " block hashes into hasher");
                for (long bn = 0; bn <= effectiveHighest; bn++) {
                    byte[] hash = blockRegistry.getBlockHash(bn);
                    streamingHasher.addNodeByHash(hash);
                }
                System.out.println("[WRAP] Hasher replay complete. leafCount=" + streamingHasher.leafCount());
            }

            // Build validation list (same as ValidateBlocksCommand)
            BlockChainValidation chainValidation = new BlockChainValidation();
            HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chainValidation);
            HbarSupplyValidation supplyValidation = new HbarSupplyValidation();

            final NodeStakeRegistry nodeStakeRegistry = new NodeStakeRegistry();
            List<BlockValidation> parallelValidations = new ArrayList<>();
            parallelValidations.add(new RequiredItemsValidation());
            parallelValidations.add(new BlockStructureValidation());
            SignatureValidation signatureValidation =
                    new SignatureValidation(addressBookRegistry, nodeStakeRegistry, true);
            parallelValidations.add(signatureValidation);

            // Create signature stats collector for CSV output
            final SignatureStatsCollector statsCollector =
                    new SignatureStatsCollector(wrapOutputDir.resolve("signature_statistics_live_sequential.csv"));

            List<BlockValidation> sequentialValidations = new ArrayList<>();
            sequentialValidations.add(new AddressBookUpdateValidation(addressBookRegistry));
            sequentialValidations.add(new NodeStakeUpdateValidation(nodeStakeRegistry));
            final TssEnablementRegistry tssRegistry = new TssEnablementRegistry();
            final Path tssParametersBinPath = wrapOutputDir.resolve("tss-enablement.bin");
            sequentialValidations.add(new TssEnablementValidation(tssRegistry, tssParametersBinPath));
            sequentialValidations.add(chainValidation);
            sequentialValidations.add(treeValidation);
            sequentialValidations.add(supplyValidation);
            sequentialValidations.add(new HashRegistryValidation(blockRegistry, chainValidation));
            sequentialValidations.add(new StreamingMerkleTreeValidation(streamingMerkleTreeFile, treeValidation));
            Path jumpstartPath = wrapOutputDir.resolve("jumpstart.bin");
            sequentialValidations.add(new JumpstartValidation(jumpstartPath, treeValidation, blockRegistry));
            // TODO: BalanceCheckpointValidation is intentionally excluded. It requires genesis start and
            // sourcing monthly balance checkpoint files, which adds significant complexity in live mode.
            // See ValidateBlocksCommand for the full pattern if this is needed in the future.

            List<BlockValidation> allValidations = new ArrayList<>();
            allValidations.addAll(parallelValidations);
            allValidations.addAll(sequentialValidations);

            // Filter out genesis-required validations when not starting from block 0
            boolean startingFromGenesis = (effectiveHighest < 0);
            if (!startingFromGenesis) {
                // Try to load checkpoint state, but only if it's not ahead of the wrap state.
                // Mid-zip truncation can push effectiveHighest behind the checkpoint — loading
                // a checkpoint that is ahead would cause hash mismatches.
                boolean hasCheckpoint = false;
                try {
                    Path progressFile = checkpointDir.resolve("validateProgress.json");
                    if (Files.exists(progressFile)) {
                        String cpJson = Files.readString(progressFile, StandardCharsets.UTF_8);
                        com.google.gson.JsonObject cpRoot =
                                com.google.gson.JsonParser.parseString(cpJson).getAsJsonObject();
                        long cpBlock = cpRoot.get("lastValidatedBlockNumber").getAsLong();

                        if (cpBlock <= effectiveHighest) {
                            for (BlockValidation v : allValidations) {
                                try {
                                    v.load(checkpointDir);
                                } catch (Exception e) {
                                    System.err.println(
                                            "[WRAP] Warning: could not load " + v.name() + " state: " + e.getMessage());
                                }
                            }
                            hasCheckpoint = true;
                        } else {
                            System.out.println("[WRAP] Validate checkpoint (block " + cpBlock
                                    + ") is ahead of wrap effective highest (" + effectiveHighest
                                    + "); skipping checkpoint load");
                            // Initialize chain validation from block hash registry so it
                            // doesn't treat the first block as genesis
                            byte[] lastHash = blockRegistry.getBlockHash(effectiveHighest);
                            if (lastHash != null) {
                                chainValidation.setPreviousBlockHash(lastHash);
                                System.out.println("[WRAP] Initialized chain validation from registry at block "
                                        + effectiveHighest);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[WRAP] Warning: could not load checkpoint: " + e.getMessage());
                }

                if (!hasCheckpoint) {
                    List<BlockValidation> genesisSkipped = new ArrayList<>();
                    sequentialValidations.removeIf(v -> {
                        if (v.requiresGenesisStart()) {
                            genesisSkipped.add(v);
                            return true;
                        }
                        return false;
                    });
                    allValidations.removeIf(genesisSkipped::contains);
                    for (BlockValidation skipped : genesisSkipped) {
                        System.out.println("[WRAP] Skipping: " + skipped.name() + " (requires genesis start)");
                    }
                }
            }

            // Zip writing state
            BlockZipAppender currentZip = null;
            Path currentZipPath = null;
            long durableWatermark = watermark;
            long blocksSinceWatermarkFlush = 0;
            long blocksValidated = 0;
            long lastCheckpointSaveMs = System.currentTimeMillis();

            // Tar archive state (moved from download thread to free it for faster downloading)
            ConcurrentTarZstdWriter currentDayWriter = null;
            LocalDate currentTarDay = null;

            try {
                while (true) {
                    ValidatedBlock vb = queue.take();
                    if (vb == POISON_PILL) {
                        break;
                    }

                    long blockNum = vb.blockNumber();

                    // Write to tar.zstd archive (handles day boundaries)
                    LocalDate blockDay = vb.blockDay();
                    if (blockDay != null && !blockDay.equals(currentTarDay)) {
                        if (currentDayWriter != null) {
                            currentDayWriter.close();
                            currentDayWriter = null;
                        }
                        currentTarDay = blockDay;
                        Path dayArchive = outputDir.toPath().resolve(currentTarDay + ".tar.zstd");
                        if (!Files.exists(dayArchive)) {
                            currentDayWriter = new ConcurrentTarZstdWriter(dayArchive);
                        }
                    }
                    if (currentDayWriter != null) {
                        for (InMemoryFile file : vb.files()) {
                            currentDayWriter.putEntry(file);
                        }
                    }

                    // Skip if already wrapped
                    if (blockNum <= effectiveHighest) {
                        continue;
                    }

                    // Classify files and build UnparsedRecordBlock
                    DownloadDayLiveImpl.BlockDownloadResult downloadResult =
                            new DownloadDayLiveImpl.BlockDownloadResult(blockNum, vb.files(), vb.runningHash());
                    InMemoryFile primaryRecord = findPrimaryRecord(downloadResult);
                    List<InMemoryFile> signatures = findSignatures(downloadResult);
                    List<InMemoryFile> sidecars = findSidecars(downloadResult, primaryRecord);

                    if (primaryRecord == null) {
                        throw new IllegalStateException("No primary record found for block " + blockNum);
                    }

                    UnparsedRecordBlock unparsedBlock = UnparsedRecordBlock.newInMemoryBlock(
                            vb.recordFileTime(), primaryRecord, List.of(), signatures, sidecars, List.of());

                    // Parse and verify signatures
                    ParsedRecordBlock parsedBlock = unparsedBlock.parse();
                    NodeAddressBook ab = addressBookRegistry.getAddressBookForBlock(parsedBlock.blockTime());
                    byte[] signedHash = parsedBlock.recordFile().signedHash();
                    List<RecordFileSignature> verifiedSigs = parsedBlock.signatureFiles().stream()
                            .filter(psf -> psf.isValid(signedHash, ab))
                            .map(psf -> psf.toRecordFileSignature(ab))
                            .toList();

                    PreVerifiedBlock preVerified = new PreVerifiedBlock(parsedBlock, ab, verifiedSigs);

                    // Address book auto-update
                    try {
                        preVerified = updateAddressBookAndReverify(preVerified, blockNum, addressBookRegistry);
                    } catch (Exception e) {
                        System.err.printf(
                                "[WRAP] Warning: address book auto-update failed at block %d: %s%n", blockNum, e);
                    }

                    // Convert to wrapped block
                    Block wrapped = RecordBlockConverter.toBlock(
                            preVerified,
                            blockNum,
                            blockRegistry.mostRecentBlockHash(),
                            streamingHasher.computeRootHash(),
                            amendmentProvider);

                    // Hash and update chain state
                    byte[] blockStreamBlockHash = hashBlock(wrapped);
                    streamingHasher.addNodeByHash(blockStreamBlockHash);
                    blockRegistry.addBlock(blockNum, blockStreamBlockHash);

                    // Compute block path and write to zip
                    BlockPath blockPath =
                            BlockWriter.computeBlockPath(wrapOutputDir, blockNum, BlockArchiveType.UNCOMPRESSED_ZIP);
                    Files.createDirectories(blockPath.dirPath());

                    Bytes wrappedBytes = Block.PROTOBUF.toBytes(wrapped);
                    byte[] serializedBytes = BlockWriter.serializeBlockToBytes(wrapped, DEFAULT_COMPRESSION);

                    // Switch zip file if needed
                    if (!blockPath.zipFilePath().equals(currentZipPath)) {
                        if (currentZip != null) {
                            currentZip.close();
                            saveWatermark(watermarkFile, durableWatermark);
                            blocksSinceWatermarkFlush = 0;
                        }
                        currentZip = BlockWriter.openZipForAppend(blockPath.zipFilePath());
                        currentZipPath = blockPath.zipFilePath();
                    }
                    BlockWriter.writeBlockEntry(currentZip, blockPath, serializedBytes);

                    durableWatermark = blockNum;
                    blocksSinceWatermarkFlush++;
                    if (blocksSinceWatermarkFlush >= WATERMARK_BATCH_SIZE) {
                        saveWatermark(watermarkFile, durableWatermark);
                        blocksSinceWatermarkFlush = 0;
                    }

                    // Run BlockValidation checks (reuse raw protobuf bytes, skip re-serialization)
                    BlockUnparsed blockUnparsed = BlockUnparsed.PROTOBUF.parse(
                            wrappedBytes.toReadableSequentialData(), false, false, Codec.DEFAULT_MAX_DEPTH, 37_748_736);

                    // Phase 1: Sequential validations (run first so AddressBookUpdateValidation
                    // updates the address book before SignatureValidation uses it)
                    for (BlockValidation v : sequentialValidations) {
                        try {
                            v.validate(blockUnparsed, blockNum);
                        } catch (ValidationException e) {
                            // Save checkpoint before failing
                            saveValidationCheckpoint(
                                    checkpointDir, blocksValidated, blockNum - 1, chainValidation, allValidations);
                            throw new IllegalStateException(
                                    "Validation '" + v.name() + "' failed at block " + blockNum + ": " + e.getMessage(),
                                    e);
                        }
                    }

                    // Phase 2: Parallel validations (including SignatureValidation, which now
                    // sees the up-to-date address book)
                    for (BlockValidation v : parallelValidations) {
                        try {
                            v.validate(blockUnparsed, blockNum);
                        } catch (ValidationException e) {
                            throw new IllegalStateException(
                                    "Validation '" + v.name() + "' failed at block " + blockNum + ": " + e.getMessage(),
                                    e);
                        }
                    }

                    // Phase 3: Commit state for all validations
                    for (BlockValidation v : allValidations) {
                        v.commitState(blockUnparsed, blockNum);
                    }

                    blocksValidated++;

                    // Collect signature stats from the validation
                    SignatureBlockStats blockStats = signatureValidation.popBlockStats(blockNum);
                    if (blockStats != null) {
                        statsCollector.accept(blockStats);
                    }

                    // Save jumpstart.bin every block (tiny ~1KB write)
                    saveJumpstart(jumpstartPath, blockNum, blockStreamBlockHash, streamingHasher);

                    // Periodic checkpoint save
                    long nowMs = System.currentTimeMillis();
                    if (nowMs - lastCheckpointSaveMs >= 60_000L) {
                        saveValidationCheckpoint(
                                checkpointDir, blocksValidated, blockNum, chainValidation, allValidations);
                        lastCheckpointSaveMs = nowMs;
                    }

                    // Progress
                    if (blocksValidated % PROGRESS_LOG_INTERVAL == 0) {
                        System.out.println("[WRAP] Wrapped + validated block " + blockNum + " (" + blocksValidated
                                + " total, sigs=" + verifiedSigs.size() + ")");
                    }
                }
            } finally {
                // Close zip and save state
                if (currentZip != null) {
                    try {
                        currentZip.close();
                    } catch (IOException e) {
                        System.err.println("[WRAP] Error closing zip: " + e.getMessage());
                    }
                }
                if (currentDayWriter != null) {
                    try {
                        currentDayWriter.close();
                    } catch (Exception e) {
                        System.err.println("[WRAP] Error closing tar writer: " + e.getMessage());
                    }
                }
                saveWatermark(watermarkFile, durableWatermark);
                saveStateCheckpoint(streamingMerkleTreeFile, streamingHasher);
                byte[] lastBlockHash = blockRegistry.getBlockHash(durableWatermark);
                if (lastBlockHash != null) {
                    saveJumpstart(jumpstartPath, durableWatermark, lastBlockHash, streamingHasher);
                }
                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);

                // Save validation checkpoint
                saveValidationCheckpoint(
                        checkpointDir, blocksValidated, durableWatermark, chainValidation, allValidations);

                // Finalize all validations (end-of-stream checks)
                for (BlockValidation v : allValidations) {
                    try {
                        v.finalize(blocksValidated, durableWatermark);
                    } catch (ValidationException e) {
                        System.err.println("[WRAP] Warning: finalize failed for " + v.name() + ": " + e.getMessage());
                    }
                }

                // Finalize and close signature stats collector
                statsCollector.finalizeDayStats();
                statsCollector.printFinalSummary();
                statsCollector.close();

                // Close all validations
                for (BlockValidation v : allValidations) {
                    v.close();
                }

                System.out.println("[WRAP] Shutdown complete. Wrapped " + blocksValidated + " blocks.");
            }
        }
    }

    /**
     * Saves validation checkpoint state.
     */
    private static void saveValidationCheckpoint(
            Path checkpointDir,
            long blocksValidated,
            long lastValidatedBlockNumber,
            BlockChainValidation chainValidation,
            List<BlockValidation> validations) {
        try {
            Files.createDirectories(checkpointDir);
            // Save each validation's state
            for (BlockValidation v : validations) {
                try {
                    v.save(checkpointDir);
                } catch (Exception e) {
                    System.err.println("[WRAP] Warning: could not save " + v.name() + " state: " + e.getMessage());
                }
            }
            // Save validateProgress.json
            byte[] previousBlockHash = chainValidation.getPreviousBlockHash();
            JsonObject root = new JsonObject();
            root.addProperty("schemaVersion", 3);
            root.addProperty("lastValidatedBlockNumber", lastValidatedBlockNumber);
            root.addProperty("blocksValidated", blocksValidated);
            root.addProperty(
                    "previousBlockHashHex",
                    previousBlockHash != null ? Bytes.wrap(previousBlockHash).toHex() : "");
            final String json = new GsonBuilder().setPrettyPrinting().create().toJson(root);
            HasherStateFiles.saveAtomically(checkpointDir.resolve("validateProgress.json"), path -> {
                try (var writer = Files.newBufferedWriter(path)) {
                    writer.write(json);
                }
            });
        } catch (Exception e) {
            System.err.println("[WRAP] Warning: could not save checkpoint: " + e.getMessage());
        }
    }

    /**
     * Address book auto-update logic, copied from ToWrappedBlocksCommand.
     */
    private static PreVerifiedBlock updateAddressBookAndReverify(
            PreVerifiedBlock preVerified, long blockNum, AddressBookRegistry addressBookRegistry) {
        var streamItems =
                preVerified.recordBlock().recordFile().recordStreamFile().recordStreamItems();
        List<Transaction> transactions = new ArrayList<>();
        for (var rsi : streamItems) {
            if (rsi.hasTransaction()) {
                transactions.add(rsi.transactionOrThrow());
            }
        }

        if (!transactions.isEmpty()) {
            try {
                var addressBookTxns = AddressBookRegistry.filterToJustAddressBookTransactions(transactions);
                if (!addressBookTxns.isEmpty()) {
                    Instant blockInstant = preVerified.recordBlock().blockTime();
                    String changes = addressBookRegistry.updateAddressBook(blockInstant.plusNanos(1), addressBookTxns);
                    if (changes != null) {
                        System.out.println("[WRAP] Address book updated at block " + blockNum + ": " + changes);
                    }
                }
            } catch (Exception e) {
                // Don't fail for address book parse errors
            }
        }

        // Check if address book used in verification is stale
        NodeAddressBook currentBook = addressBookRegistry.getAddressBookForBlock(
                preVerified.recordBlock().blockTime());
        if (!currentBook.equals(preVerified.addressBook())) {
            byte[] signedHash = preVerified.recordBlock().recordFile().signedHash();
            List<RecordFileSignature> reverifiedSigs = preVerified.recordBlock().signatureFiles().stream()
                    .filter(psf -> psf.isValid(signedHash, currentBook))
                    .map(psf -> psf.toRecordFileSignature(currentBook))
                    .toList();
            System.out.printf(
                    "[WRAP] Block %d: re-verified signatures with updated address book: %d verified (was %d)%n",
                    blockNum,
                    reverifiedSigs.size(),
                    preVerified.verifiedSignatures().size());
            return new PreVerifiedBlock(preVerified.recordBlock(), currentBook, reverifiedSigs);
        }

        return preVerified;
    }

    /**
     * Refreshes GCS listings for all days plus a specific day.
     */
    private void refreshListingsForDay(LocalDate day, NetworkConfig netConfig) {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        boolean isToday = day.equals(today);

        UpdateDayListingsCommand.updateDayListings(
                listingDir.toPath(),
                CACHE_DIR.toPath(),
                true,
                netConfig.minNodeAccountId(),
                netConfig.maxNodeAccountId(),
                DownloadConstants.GCP_PROJECT_ID);

        UpdateDayListingsCommand.updateListingsForSingleDay(
                listingDir.toPath(),
                CACHE_DIR.toPath(),
                !isToday,
                netConfig.minNodeAccountId(),
                netConfig.maxNodeAccountId(),
                DownloadConstants.GCP_PROJECT_ID,
                day);
    }

    /**
     * Refreshes GCS listings for only the given day (skips the global all-days refresh).
     */
    private void refreshListingsForSingleDay(LocalDate day, NetworkConfig netConfig) {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        boolean isToday = day.equals(today);

        UpdateDayListingsCommand.updateListingsForSingleDay(
                listingDir.toPath(),
                CACHE_DIR.toPath(),
                !isToday,
                netConfig.minNodeAccountId(),
                netConfig.maxNodeAccountId(),
                DownloadConstants.GCP_PROJECT_ID,
                day);
    }

    /** Resolve the ordered download file list from a group of listing files. */
    private static List<ListingRecordFile> resolveOrderedFiles(List<ListingRecordFile> group) {
        ListingRecordFile mostCommonRecord = RecordFileUtils.findMostCommonByType(group, ListingRecordFile.Type.RECORD);
        ListingRecordFile[] mostCommonSidecars = RecordFileUtils.findMostCommonSidecars(group);
        return DownloadDayLiveImpl.computeFilesToDownload(mostCommonRecord, mostCommonSidecars, group);
    }

    /** Resolve the set of most-common files (record + sidecars) for path deduplication. */
    private static Set<ListingRecordFile> resolveMostCommonFilesSet(List<ListingRecordFile> group) {
        Set<ListingRecordFile> set = new HashSet<>();
        ListingRecordFile mostCommonRecord = RecordFileUtils.findMostCommonByType(group, ListingRecordFile.Type.RECORD);
        if (mostCommonRecord != null) set.add(mostCommonRecord);
        ListingRecordFile[] mostCommonSidecars = RecordFileUtils.findMostCommonSidecars(group);
        for (ListingRecordFile sidecar : mostCommonSidecars) {
            if (sidecar != null) set.add(sidecar);
        }
        return set;
    }

    /** Fire parallel downloads for all files in the ordered list. */
    private static List<CompletableFuture<InMemoryFile>> fireDownloads(
            List<ListingRecordFile> orderedFiles,
            NetworkConfig netConfig,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager) {
        List<CompletableFuture<InMemoryFile>> futures = new ArrayList<>(orderedFiles.size());
        for (ListingRecordFile lr : orderedFiles) {
            String blobName = netConfig.bucketPathPrefix() + lr.path();
            futures.add(downloadManager.downloadAsync(netConfig.gcsBucketName(), blobName));
        }
        return futures;
    }

    /**
     * Saves state to JSON file.
     */
    private void saveState(State state) {
        try {
            Path parentDir = stateJsonPath.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }
            String json = GSON.toJson(state);
            HasherStateFiles.saveAtomically(
                    stateJsonPath, path -> Files.writeString(path, json, StandardCharsets.UTF_8));
        } catch (Exception e) {
            System.err.println("[live-sequential] Warning: Failed to save state: " + e.getMessage());
        }
    }

    /** Load the durable commit watermark from the given file. */
    private static long loadWatermark(Path watermarkFile) {
        if (!Files.exists(watermarkFile)) {
            return -1;
        }
        try {
            byte[] bytes = Files.readAllBytes(watermarkFile);
            if (bytes.length < Long.BYTES) {
                return -1;
            }
            return ByteBuffer.wrap(bytes).getLong();
        } catch (IOException e) {
            System.err.println("Warning: could not read watermark file: " + e.getMessage());
            return -1;
        }
    }

    /** Save the durable commit watermark atomically (write to .tmp, then rename). */
    private static void saveWatermark(Path watermarkFile, long blockNumber) {
        if (blockNumber < 0) {
            return;
        }
        Path tmpFile = watermarkFile.resolveSibling(watermarkFile.getFileName() + ".tmp");
        try {
            byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(blockNumber).array();
            Files.write(tmpFile, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpFile, watermarkFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Warning: could not save watermark: " + e.getMessage());
        }
    }

    /**
     * Save jumpstart.bin atomically. Format matches what {@link JumpstartValidation} reads:
     * block number (long), block hash (48 bytes), leaf count (long), hash count (int),
     * followed by hash count × 48-byte hashes (streaming hasher intermediate state).
     */
    private static void saveJumpstart(
            Path jumpstartPath, long blockNumber, byte[] blockHash, StreamingHasher streamingHasher) {
        try {
            HasherStateFiles.saveAtomically(jumpstartPath, path -> {
                try (var out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(path), 8192))) {
                    out.writeLong(blockNumber);
                    out.write(blockHash);
                    out.writeLong(streamingHasher.leafCount());
                    List<byte[]> hashes = streamingHasher.intermediateHashingState();
                    out.writeInt(hashes.size());
                    for (byte[] h : hashes) {
                        out.write(h);
                    }
                }
            });
        } catch (Exception e) {
            System.err.println("[WRAP] Warning: could not save jumpstart.bin: " + e.getMessage());
        }
    }

    private static String formatDuration(long seconds) {
        if (seconds < 60) {
            return seconds + "s";
        } else if (seconds < 3600) {
            return String.format("%dm %ds", seconds / 60, seconds % 60);
        } else if (seconds < 86400) {
            return String.format("%dh %dm", seconds / 3600, (seconds % 3600) / 60);
        } else {
            return String.format("%dd %dh", seconds / 86400, (seconds % 86400) / 3600);
        }
    }
}
