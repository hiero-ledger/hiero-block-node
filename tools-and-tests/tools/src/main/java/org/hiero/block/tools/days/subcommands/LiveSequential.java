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
import static org.hiero.block.tools.days.downloadlive.ValidateDownloadLive.fullBlockValidate;
import static org.hiero.block.tools.utils.Md5Checker.checkMd5;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.pbj.runtime.io.buffer.Bytes;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.tools.blocks.AmendmentProvider;
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
import org.hiero.block.tools.blocks.validation.RequiredItemsValidation;
import org.hiero.block.tools.blocks.validation.SignatureValidation;
import org.hiero.block.tools.blocks.validation.StreamingMerkleTreeValidation;
import org.hiero.block.tools.config.NetworkConfig;
import org.hiero.block.tools.days.download.DownloadConstants;
import org.hiero.block.tools.days.download.DownloadDayLiveImpl;
import org.hiero.block.tools.days.listing.DayListingFileReader;
import org.hiero.block.tools.days.listing.ListingRecordFile;
import org.hiero.block.tools.days.model.AddressBookRegistry;
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
    private static final long MIN_BLOCK_TIME_REFRESH_INTERVAL_MS = 30_000;
    private static final int WATERMARK_BATCH_SIZE = 256;

    private static final File CACHE_DIR = new File("metadata/gcp-cache");

    /** Poison pill to signal the wrap+validate thread to exit. */
    private static final ValidatedBlock POISON_PILL = new ValidatedBlock(-1, null, List.of(), null);

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

    @Option(
            names = {"--stats-csv"},
            description = "Path to signature statistics CSV file (default: outputDir/signature_statistics.csv)")
    private Path statsCsvPath;

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

    /** Mutable state for the download loop, enabling extraction of the loop body into a helper method. */
    private static class DownloadLoopState {
        long currentBlockNumber;
        byte[] currentHash;
        LocalDate currentDay;
        BlockTimeReader blockTimeReader;
        ConcurrentTarZstdWriter dayWriter;
        long blocksProcessedTotal;
        long blocksProcessedToday;
        long dayStartTime;
        long lastBlockTimeRefreshMs;
        List<ListingRecordFile> cachedListingFiles;
        LocalDate cachedListingDay;
        final NetworkConfig netConfig;

        DownloadLoopState(State initialState, BlockTimeReader reader) {
            this.currentBlockNumber = initialState.blockNumber;
            this.currentHash = initialState.getHashBytes();
            this.currentDay = initialState.getDayDate();
            this.blockTimeReader = reader;
            this.dayStartTime = System.currentTimeMillis();
            this.netConfig = NetworkConfig.current();
        }
    }

    /** Immutable container for the three validation lists used by the wrap+validate thread. */
    private record WrapValidations(
            List<BlockValidation> parallel, List<BlockValidation> sequential, List<BlockValidation> all) {}

    /** Block data passed from the download thread to the wrap+validate thread. */
    private record ValidatedBlock(
            long blockNumber, Instant recordFileTime, List<InMemoryFile> files, byte[] runningHash) {}

    @Override
    public void run() {
        logConfiguration();
        try {
            initializeDirectoriesAndDefaults();
            final AddressBookRegistry addressBookRegistry =
                    addressBookPath != null ? new AddressBookRegistry(addressBookPath) : new AddressBookRegistry();
            final Storage storage = StorageOptions.http()
                    .setProjectId(DownloadConstants.GCP_PROJECT_ID)
                    .build()
                    .getService();
            final ConcurrentDownloadManagerVirtualThreadsV3 downloadManager =
                    ConcurrentDownloadManagerVirtualThreadsV3.newBuilder(storage)
                            .setMaxConcurrency(16)
                            .build();
            final BlockTimeReader blockTimeReader = new BlockTimeReader();
            final State initialState = determineStartingPoint(blockTimeReader);
            logStartingPoint(initialState);

            final BlockingQueue<ValidatedBlock> queue = new LinkedBlockingQueue<>(32);
            final AtomicReference<Throwable> wrapError = new AtomicReference<>(null);
            final Thread wrapThread = startWrapThread(queue, addressBookRegistry, wrapError);
            addShutdownHook(downloadManager);

            processBlocksSequentially(
                    initialState, addressBookRegistry, downloadManager, blockTimeReader, queue, wrapError);

            queue.put(POISON_PILL);
            wrapThread.join(60_000);
            Throwable wrapErr = wrapError.get();
            if (wrapErr != null) {
                throw new IllegalStateException("Wrap+validate thread failed", wrapErr);
            }
            System.out.println("[live-sequential] Complete.");
        } catch (Exception e) {
            System.err.println("[live-sequential] Fatal error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void logConfiguration() {
        System.out.println("[live-sequential] Starting sequential block download with inline wrapping + validation");
        System.out.println("Configuration:");
        System.out.println("  listingDir=" + listingDir);
        System.out.println("  outputDir=" + outputDir);
        System.out.println("  wrapOutputDir=" + wrapOutputDir);
        System.out.println("  stateJsonPath=" + stateJsonPath);
        System.out.println("  addressBookPath=" + addressBookPath);
        System.out.println("  startDate=" + (startDate != null ? startDate : "(auto-detect)"));
    }

    private void initializeDirectoriesAndDefaults() throws IOException {
        Files.createDirectories(outputDir.toPath());
        Files.createDirectories(wrapOutputDir);
        if (stateJsonPath == null) {
            stateJsonPath = outputDir.toPath().resolve("validateCmdStatus.json");
        }
        if (stateJsonPath.getParent() != null) {
            Files.createDirectories(stateJsonPath.getParent());
        }
        if (statsCsvPath == null) {
            statsCsvPath = outputDir.toPath().resolve("signature_statistics.csv");
        }
    }

    private static void logStartingPoint(State initialState) {
        System.out.println("[live-sequential] Starting from block " + initialState.blockNumber
                + " hash["
                + (initialState.endRunningHashHex != null
                        ? initialState.endRunningHashHex.substring(
                                0, Math.min(8, initialState.endRunningHashHex.length()))
                        : "null")
                + "]"
                + " day=" + initialState.dayDate);
    }

    private Thread startWrapThread(
            BlockingQueue<ValidatedBlock> queue,
            AddressBookRegistry addressBookRegistry,
            AtomicReference<Throwable> wrapError) {
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
        return wrapThread;
    }

    private static void addShutdownHook(ConcurrentDownloadManagerVirtualThreadsV3 downloadManager) {
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            System.out.println("[live-sequential] Shutdown requested...");
                            downloadManager.close();
                        },
                        "live-sequential-shutdown"));
    }

    /**
     * Determines the starting point for block processing.
     * Priority: 1) Wrap effective highest (accounts for mid-zip truncation), 2) Resume from state file,
     * 3) Use --start-date, 4) Auto-detect from mirror node
     */
    @SuppressWarnings("PMD.NPathComplexity") // Multiple priority levels require branching
    private State determineStartingPoint(BlockTimeReader blockTimeReader) {
        // Priority 1: Resume from wrap state (the authoritative source, accounts for mid-zip truncation)
        long wrapEffective = computeWrapEffectiveHighest();
        if (wrapEffective >= 0) {
            System.out.println("[live-sequential] Resuming from wrap effective highest: block " + wrapEffective);
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
            throw new IllegalStateException("Failed to get latest block from mirror node");
        }

        BlockInfo latestBlock = latestBlocks.getFirst();
        String timestamp = latestBlock.timestampFrom != null ? latestBlock.timestampFrom : latestBlock.timestampTo;

        if (timestamp == null) {
            throw new IllegalStateException("Latest block has no timestamp");
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
            } else if (watermark < 0 && registryHighest >= 0) {
                // trust registry
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
    @SuppressWarnings("PMD.NPathComplexity")
    private void processBlocksSequentially(
            State initialState,
            AddressBookRegistry addressBookRegistry,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockTimeReader initialBlockTimeReader,
            BlockingQueue<ValidatedBlock> queue,
            AtomicReference<Throwable> wrapError)
            throws Exception {
        final DownloadLoopState state = new DownloadLoopState(initialState, initialBlockTimeReader);
        try {
            while (true) {
                Throwable wrapErr = wrapError.get();
                if (wrapErr != null) {
                    throw new IllegalStateException("Wrap+validate thread failed, stopping download", wrapErr);
                }
                processOneBlock(state, addressBookRegistry, downloadManager, queue);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[live-sequential] Interrupted, saving state...");
        } finally {
            saveFinalState(state.currentBlockNumber, state.currentHash, state.currentDay, state.blockTimeReader);
            closeQuietly(state.dayWriter);
        }
    }

    private void processOneBlock(
            DownloadLoopState state,
            AddressBookRegistry addressBookRegistry,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            BlockingQueue<ValidatedBlock> queue)
            throws Exception {
        long nextBlockNumber = state.currentBlockNumber + 1;
        LocalDateTime blockTime = resolveBlockTime(state.blockTimeReader, nextBlockNumber);
        if (blockTime == null) {
            state.blockTimeReader =
                    refreshBlockTimeIfNeeded(state.blockTimeReader, nextBlockNumber, state.lastBlockTimeRefreshMs);
            state.lastBlockTimeRefreshMs = System.currentTimeMillis();
            Thread.sleep(LIVE_POLL_INTERVAL.toMillis());
            return;
        }
        LocalDate blockDay = blockTime.toLocalDate();
        if (!blockDay.equals(state.currentDay)) {
            closeDayWriter(state.dayWriter, state.currentDay, state.blocksProcessedToday, state.dayStartTime);
            state.dayWriter = openDayWriterIfNeeded(blockDay);
            state.currentDay = blockDay;
            state.blocksProcessedToday = 0;
            state.dayStartTime = System.currentTimeMillis();
            state.cachedListingFiles = null;
            state.cachedListingDay = null;
        } else if (state.dayWriter == null && state.currentDay != null) {
            state.dayWriter = openDayWriterIfNeeded(state.currentDay);
            if (state.dayWriter != null) state.dayStartTime = System.currentTimeMillis();
        }
        if (!blockDay.equals(state.cachedListingDay)) {
            state.cachedListingFiles = loadListingsWithRetry(blockDay, blockTime, state.netConfig);
            state.cachedListingDay = blockDay;
        }
        List<ListingRecordFile> group = findBlockGroupWithRefresh(
                blockTime, blockDay, nextBlockNumber, state.cachedListingFiles, state.netConfig);
        if (group == null) {
            state.blockTimeReader = new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
            return;
        }
        state.cachedListingFiles = reloadListings(blockTime);
        state.cachedListingDay = blockDay;
        byte[] resolvedHash = fetchPreviousHashIfNeeded(state.currentHash, nextBlockNumber);
        List<InMemoryFile> inMemoryFiles = downloadBlockFiles(group, state.netConfig, downloadManager, nextBlockNumber);
        byte[] newHash = DownloadDayLiveImpl.validateBlockHashes(nextBlockNumber, inMemoryFiles, resolvedHash, null);
        validateBlock(addressBookRegistry, resolvedHash, blockTime, nextBlockNumber, inMemoryFiles, newHash);
        assertSequentialOrdering(nextBlockNumber, state.currentBlockNumber);
        writeToDayArchive(state.dayWriter, inMemoryFiles);
        Instant recordFileTime = blockTime.atZone(ZoneOffset.UTC).toInstant();
        queue.put(new ValidatedBlock(nextBlockNumber, recordFileTime, inMemoryFiles, newHash));
        state.currentBlockNumber = nextBlockNumber;
        state.currentHash = newHash;
        state.blocksProcessedTotal++;
        state.blocksProcessedToday++;
        saveState(new State(state.currentBlockNumber, state.currentHash, recordFileTime, state.currentDay));
        logProgress(
                state.blocksProcessedTotal, state.blocksProcessedToday, state.currentBlockNumber, state.dayStartTime);
    }

    private static BlockTimeReader refreshBlockTimeIfNeeded(
            BlockTimeReader reader, long blockNumber, long lastRefreshMs) throws IOException {
        long now = System.currentTimeMillis();
        if (now - lastRefreshMs >= MIN_BLOCK_TIME_REFRESH_INTERVAL_MS) {
            System.out.println("[LIVE] Block " + blockNumber + " not in BlockTimeReader, refreshing...");
            UpdateBlockData.updateMirrorNodeData(MetadataFiles.BLOCK_TIMES_FILE, MetadataFiles.DAY_BLOCKS_FILE);
            return new BlockTimeReader(MetadataFiles.BLOCK_TIMES_FILE);
        }
        return reader;
    }

    private static LocalDateTime resolveBlockTime(BlockTimeReader reader, long blockNumber) {
        try {
            return reader.getBlockLocalDateTime(blockNumber);
        } catch (Exception e) {
            return null;
        }
    }

    private List<ListingRecordFile> findBlockGroupWithRefresh(
            LocalDateTime blockTime,
            LocalDate blockDay,
            long blockNumber,
            List<ListingRecordFile> cachedListingFiles,
            NetworkConfig netConfig)
            throws Exception {
        List<ListingRecordFile> group = findBlockGroup(blockTime, cachedListingFiles);
        if (group != null) return group;

        refreshListingsForDay(blockDay, netConfig);
        List<ListingRecordFile> refreshed = reloadListings(blockTime);
        group = findBlockGroup(blockTime, refreshed);
        if (group != null) return group;

        System.out.println("[live-sequential] No files found for block " + blockNumber + " at time " + blockTime
                + ", fixing block times...");
        FixBlockTime.fixBlockTimeRange(MetadataFiles.BLOCK_TIMES_FILE, blockNumber, blockNumber + 100);
        return null;
    }

    private static void closeDayWriter(
            ConcurrentTarZstdWriter writer, LocalDate day, long blocksToday, long dayStartTime) throws Exception {
        if (writer != null) {
            writer.close();
            System.out.println("[live-sequential] Day completed: " + day + " (" + blocksToday + " blocks in "
                    + formatDuration((System.currentTimeMillis() - dayStartTime) / 1000) + ")");
        }
    }

    private ConcurrentTarZstdWriter openDayWriterIfNeeded(LocalDate day) throws Exception {
        Path dayArchive = outputDir.toPath().resolve(day + ".tar.zstd");
        if (Files.exists(dayArchive)) {
            System.out.println("[live-sequential] Day archive already exists, skipping tar writes: " + day);
            return null;
        }
        return new ConcurrentTarZstdWriter(dayArchive);
    }

    private List<ListingRecordFile> loadListingsWithRetry(
            LocalDate blockDay, LocalDateTime blockTime, NetworkConfig netConfig) throws Exception {
        int attempt = 0;
        while (true) {
            attempt++;
            refreshListingsForDay(blockDay, netConfig);
            try {
                List<ListingRecordFile> listings = DayListingFileReader.loadRecordsFileForDay(
                        listingDir.toPath(), blockTime.getYear(), blockTime.getMonthValue(), blockTime.getDayOfMonth());
                System.out.println("[live-sequential] Loaded " + listings.size() + " listing entries for " + blockDay);
                return listings;
            } catch (NoSuchFileException e) {
                System.out.println("[live-sequential] Listings not available yet for " + blockDay
                        + ", waiting 15 minutes... (attempt " + attempt + ")");
                Thread.sleep(15 * 60 * 1000);
            }
        }
    }

    private List<ListingRecordFile> reloadListings(LocalDateTime blockTime) throws Exception {
        return DayListingFileReader.loadRecordsFileForDay(
                listingDir.toPath(), blockTime.getYear(), blockTime.getMonthValue(), blockTime.getDayOfMonth());
    }

    private static List<ListingRecordFile> findBlockGroup(LocalDateTime blockTime, List<ListingRecordFile> listings) {
        Map<LocalDateTime, List<ListingRecordFile>> filesByBlock =
                listings.stream().collect(Collectors.groupingBy(ListingRecordFile::timestamp));
        List<ListingRecordFile> group = filesByBlock.get(blockTime);
        return (group == null || group.isEmpty()) ? null : group;
    }

    private static byte[] fetchPreviousHashIfNeeded(byte[] currentHash, long nextBlockNumber) {
        if (currentHash == null && nextBlockNumber > 0) {
            System.out.println(
                    "[live-sequential] Fetching previous hash from mirror node for block " + nextBlockNumber);
            try {
                var prevHashBytes = FetchBlockQuery.getPreviousHashForBlock(nextBlockNumber);
                byte[] hash = prevHashBytes.toByteArray();
                System.out.println("[live-sequential] Got previous hash: "
                        + HexFormat.of().formatHex(hash).substring(0, 16) + "...");
                return hash;
            } catch (Exception e) {
                System.err.println("[live-sequential] Warning: Could not fetch previous hash: " + e.getMessage());
            }
        }
        return currentHash;
    }

    private List<InMemoryFile> downloadBlockFiles(
            List<ListingRecordFile> group,
            NetworkConfig netConfig,
            ConcurrentDownloadManagerVirtualThreadsV3 downloadManager,
            long blockNumber)
            throws Exception {
        ListingRecordFile mostCommonRecord = RecordFileUtils.findMostCommonByType(group, ListingRecordFile.Type.RECORD);
        ListingRecordFile[] mostCommonSidecars = RecordFileUtils.findMostCommonSidecars(group);
        List<ListingRecordFile> orderedFiles =
                DownloadDayLiveImpl.computeFilesToDownload(mostCommonRecord, mostCommonSidecars, group);
        Set<ListingRecordFile> mostCommonFilesSet = new HashSet<>();
        if (mostCommonRecord != null) mostCommonFilesSet.add(mostCommonRecord);
        ListingRecordFile mostCommonSidecarSingle =
                RecordFileUtils.findMostCommonByType(group, ListingRecordFile.Type.RECORD_SIDECAR);
        if (mostCommonSidecarSingle != null) mostCommonFilesSet.add(mostCommonSidecarSingle);

        List<InMemoryFile> inMemoryFiles = new ArrayList<>();
        for (ListingRecordFile lr : orderedFiles) {
            String blobName = netConfig.bucketPathPrefix() + lr.path();
            try {
                InMemoryFile downloadedFile = downloadManager
                        .downloadAsync(netConfig.gcsBucketName(), blobName)
                        .join();
                String filename = lr.path().substring(lr.path().lastIndexOf('/') + 1);
                if (!checkMd5(lr.md5Hex(), downloadedFile.data())) {
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
                System.err.println("[live-sequential] Download failed for " + blobName + ": " + ce.getMessage());
                throw new IllegalStateException("Download failed for block " + blockNumber, ce.getCause());
            }
        }
        return inMemoryFiles;
    }

    private static void validateBlock(
            AddressBookRegistry addressBookRegistry,
            byte[] currentHash,
            LocalDateTime blockTime,
            long blockNumber,
            List<InMemoryFile> files,
            byte[] newHash)
            throws Exception {
        DownloadDayLiveImpl.BlockDownloadResult result =
                new DownloadDayLiveImpl.BlockDownloadResult(blockNumber, files, newHash);
        Instant recordFileTime = blockTime.atZone(ZoneOffset.UTC).toInstant();
        if (!fullBlockValidate(addressBookRegistry, currentHash, recordFileTime, result, null)) {
            throw new IllegalStateException("Full block validation failed for block " + blockNumber + " — aborting");
        }
    }

    private static void assertSequentialOrdering(long nextBlockNumber, long currentBlockNumber) {
        if (nextBlockNumber != currentBlockNumber + 1) {
            throw new IllegalStateException(
                    "Block gap detected: expected " + (currentBlockNumber + 1) + " but got " + nextBlockNumber);
        }
    }

    private static void writeToDayArchive(ConcurrentTarZstdWriter writer, List<InMemoryFile> files) throws IOException {
        if (writer != null) {
            for (InMemoryFile file : files) {
                writer.putEntry(file);
            }
        }
    }

    private static void logProgress(long total, long today, long blockNumber, long dayStartTime) {
        if (total % PROGRESS_LOG_INTERVAL == 0) {
            long elapsed = System.currentTimeMillis() - dayStartTime;
            double blocksPerSec = today / Math.max(1.0, elapsed / 1000.0);
            System.out.println("[live-sequential] Block " + blockNumber + " (" + today + " today, "
                    + String.format("%.1f", blocksPerSec) + " blocks/sec)");
        }
    }

    private void saveFinalState(long blockNumber, byte[] hash, LocalDate day, BlockTimeReader blockTimeReader) {
        if (hash != null) {
            try {
                LocalDateTime finalBlockTime = blockTimeReader.getBlockLocalDateTime(blockNumber);
                Instant finalRecordTime = finalBlockTime.atZone(ZoneOffset.UTC).toInstant();
                saveState(new State(blockNumber, hash, finalRecordTime, day));
                System.out.println("[live-sequential] Saved state at block " + blockNumber);
            } catch (Exception e) {
                System.err.println("[live-sequential] Error saving final state: " + e.getMessage());
            }
        }
    }

    private static void closeQuietly(ConcurrentTarZstdWriter writer) {
        if (writer != null) {
            try {
                writer.close();
            } catch (Exception e) {
                System.err.println("[live-sequential] Error closing writer: " + e.getMessage());
            }
        }
    }

    /**
     * Wrap+validate consumer thread. Takes downloaded blocks from the queue, wraps them into
     * block stream format, and runs all 11 BlockValidation checks.
     */
    @SuppressWarnings("PMD.NPathComplexity")
    private void runWrapAndValidateThread(BlockingQueue<ValidatedBlock> queue, AddressBookRegistry addressBookRegistry)
            throws Exception {
        final Path streamingMerkleTreeFile = wrapOutputDir.resolve("streamingMerkleTree.bin");
        final Path watermarkFile = wrapOutputDir.resolve("wrap-commit.bin");
        final Path addressBookFile = wrapOutputDir.resolve("addressBookHistory.json");
        final Path checkpointDir = wrapOutputDir.resolve("validateCheckpoint");

        initializeWrapDirectories(addressBookFile, checkpointDir);
        final AmendmentProvider amendmentProvider =
                createAmendmentProvider(NetworkConfig.current().networkName());
        try (BlockStreamBlockHashRegistry blockRegistry =
                new BlockStreamBlockHashRegistry(wrapOutputDir.resolve("blockStreamBlockHashes.bin"))) {
            long[] state = reconcileWrapState(blockRegistry, watermarkFile);
            long effectiveHighest = state[0];

            System.out.println("[WRAP] Starting from block: " + effectiveHighest);
            final StreamingHasher streamingHasher = replayHasher(blockRegistry, effectiveHighest);

            WrapValidations wv = setupValidations(
                    addressBookRegistry, blockRegistry, streamingMerkleTreeFile, effectiveHighest, checkpointDir);

            long[] zipState = {state[1], 0};
            BlockZipAppender[] zipHolder = {null};
            Path[] zipPathHolder = {null};
            long blocksValidated = 0;
            long lastCheckpointSaveMs = System.currentTimeMillis();

            try {
                while (true) {
                    ValidatedBlock vb = queue.take();
                    if (vb == POISON_PILL) break;
                    long blockNum = vb.blockNumber();
                    if (blockNum <= effectiveHighest) continue;

                    PreVerifiedBlock preVerified = parseAndVerifyBlock(vb, addressBookRegistry, blockNum);
                    Block wrapped = RecordBlockConverter.toBlock(
                            preVerified,
                            blockNum,
                            blockRegistry.mostRecentBlockHash(),
                            streamingHasher.computeRootHash(),
                            amendmentProvider);

                    byte[] blockHash = hashBlock(wrapped);
                    streamingHasher.addNodeByHash(blockHash);
                    blockRegistry.addBlock(blockNum, blockHash);
                    writeToZipArchive(blockNum, wrapped, watermarkFile, zipHolder, zipPathHolder, zipState);
                    runBlockValidations(wrapped, blockNum, wv.parallel(), wv.sequential(), wv.all(), checkpointDir);
                    blocksValidated++;

                    if (System.currentTimeMillis() - lastCheckpointSaveMs >= 60_000L) {
                        saveValidationCheckpoint(checkpointDir, blockNum, wv.all());
                        lastCheckpointSaveMs = System.currentTimeMillis();
                    }
                    if (blocksValidated % PROGRESS_LOG_INTERVAL == 0) {
                        System.out.println("[WRAP] Wrapped + validated block " + blockNum + " ("
                                + blocksValidated + " total, sigs="
                                + preVerified.verifiedSignatures().size() + ")");
                    }
                }
            } finally {
                finalizeWrapState(
                        zipHolder[0],
                        zipState[0],
                        watermarkFile,
                        streamingMerkleTreeFile,
                        streamingHasher,
                        addressBookRegistry,
                        addressBookFile,
                        checkpointDir,
                        wv.all());
                System.out.println("[WRAP] Shutdown complete. Wrapped " + blocksValidated + " blocks.");
            }
        }
    }

    private WrapValidations setupValidations(
            AddressBookRegistry addressBookRegistry,
            BlockStreamBlockHashRegistry blockRegistry,
            Path streamingMerkleTreeFile,
            long effectiveHighest,
            Path checkpointDir) {
        BlockChainValidation chainValidation = new BlockChainValidation();
        List<BlockValidation> parallel = buildParallelValidations(addressBookRegistry);
        List<BlockValidation> sequential = buildSequentialValidations(
                addressBookRegistry, blockRegistry, chainValidation, streamingMerkleTreeFile);
        List<BlockValidation> all = new ArrayList<>(parallel);
        all.addAll(sequential);
        initializeCheckpointState(effectiveHighest, checkpointDir, blockRegistry, chainValidation, sequential, all);
        return new WrapValidations(parallel, sequential, all);
    }

    private void initializeWrapDirectories(Path addressBookFile, Path checkpointDir) throws IOException {
        Files.createDirectories(wrapOutputDir);
        Files.createDirectories(checkpointDir);
        if (!Files.exists(addressBookFile) && addressBookPath != null && Files.exists(addressBookPath)) {
            Files.copy(addressBookPath, addressBookFile);
        }
    }

    private static void finalizeWrapState(
            BlockZipAppender currentZip,
            long durableWatermark,
            Path watermarkFile,
            Path streamingMerkleTreeFile,
            StreamingHasher streamingHasher,
            AddressBookRegistry addressBookRegistry,
            Path addressBookFile,
            Path checkpointDir,
            List<BlockValidation> allValidations) {
        if (currentZip != null) {
            try {
                currentZip.close();
            } catch (IOException e) {
                System.err.println("[WRAP] Error closing zip: " + e.getMessage());
            }
        }
        saveWatermark(watermarkFile, durableWatermark);
        saveStateCheckpoint(streamingMerkleTreeFile, streamingHasher);
        addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
        saveValidationCheckpoint(checkpointDir, durableWatermark, allValidations);
        for (BlockValidation v : allValidations) v.close();
    }

    private void writeToZipArchive(
            long blockNum,
            Block wrapped,
            Path watermarkFile,
            BlockZipAppender[] zipHolder,
            Path[] zipPathHolder,
            long[] zipState)
            throws Exception {
        BlockPath blockPath = BlockWriter.computeBlockPath(wrapOutputDir, blockNum, BlockArchiveType.UNCOMPRESSED_ZIP);
        Files.createDirectories(blockPath.dirPath());
        byte[] serializedBytes = BlockWriter.serializeBlockToBytes(wrapped, DEFAULT_COMPRESSION);
        if (!blockPath.zipFilePath().equals(zipPathHolder[0])) {
            if (zipHolder[0] != null) {
                zipHolder[0].close();
                saveWatermark(watermarkFile, zipState[0]);
                zipState[1] = 0;
            }
            zipHolder[0] = BlockWriter.openZipForAppend(blockPath.zipFilePath());
            zipPathHolder[0] = blockPath.zipFilePath();
        }
        BlockWriter.writeBlockEntry(zipHolder[0], blockPath, serializedBytes);
        zipState[0] = blockNum;
        zipState[1]++;
        if (zipState[1] >= WATERMARK_BATCH_SIZE) {
            saveWatermark(watermarkFile, zipState[0]);
            zipState[1] = 0;
        }
    }

    private long[] reconcileWrapState(BlockStreamBlockHashRegistry blockRegistry, Path watermarkFile)
            throws IOException {
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
        return new long[] {effectiveHighest, watermark};
    }

    private static StreamingHasher replayHasher(BlockStreamBlockHashRegistry blockRegistry, long effectiveHighest) {
        StreamingHasher streamingHasher = new StreamingHasher();
        if (effectiveHighest >= 0) {
            System.out.println("[WRAP] Replaying " + (effectiveHighest + 1) + " block hashes into hasher");
            for (long bn = 0; bn <= effectiveHighest; bn++) {
                streamingHasher.addNodeByHash(blockRegistry.getBlockHash(bn));
            }
            System.out.println("[WRAP] Hasher replay complete. leafCount=" + streamingHasher.leafCount());
        }
        return streamingHasher;
    }

    private static List<BlockValidation> buildParallelValidations(AddressBookRegistry addressBookRegistry) {
        List<BlockValidation> validations = new ArrayList<>();
        validations.add(new RequiredItemsValidation());
        validations.add(new BlockStructureValidation());
        validations.add(new SignatureValidation(addressBookRegistry));
        return validations;
    }

    private List<BlockValidation> buildSequentialValidations(
            AddressBookRegistry addressBookRegistry,
            BlockStreamBlockHashRegistry blockRegistry,
            BlockChainValidation chainValidation,
            Path streamingMerkleTreeFile) {
        HistoricalBlockTreeValidation treeValidation = new HistoricalBlockTreeValidation(chainValidation);
        List<BlockValidation> validations = new ArrayList<>();
        validations.add(new AddressBookUpdateValidation(addressBookRegistry));
        validations.add(chainValidation);
        validations.add(treeValidation);
        validations.add(new HbarSupplyValidation());
        validations.add(new HashRegistryValidation(blockRegistry, chainValidation));
        validations.add(new StreamingMerkleTreeValidation(streamingMerkleTreeFile, treeValidation));
        Path jumpstartPath = wrapOutputDir.resolve("jumpstart.bin");
        validations.add(new JumpstartValidation(jumpstartPath, treeValidation, blockRegistry));
        return validations;
    }

    @SuppressWarnings("PMD.NPathComplexity")
    private static void initializeCheckpointState(
            long effectiveHighest,
            Path checkpointDir,
            BlockStreamBlockHashRegistry blockRegistry,
            BlockChainValidation chainValidation,
            List<BlockValidation> sequentialValidations,
            List<BlockValidation> allValidations) {
        if (effectiveHighest < 0) return; // starting from genesis

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
                    byte[] lastHash = blockRegistry.getBlockHash(effectiveHighest);
                    if (lastHash != null) {
                        chainValidation.setPreviousBlockHash(lastHash);
                        System.out.println(
                                "[WRAP] Initialized chain validation from registry at block " + effectiveHighest);
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

    private PreVerifiedBlock parseAndVerifyBlock(
            ValidatedBlock vb, AddressBookRegistry addressBookRegistry, long blockNum) throws Exception {
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
        ParsedRecordBlock parsedBlock = unparsedBlock.parse();
        NodeAddressBook ab = addressBookRegistry.getAddressBookForBlock(parsedBlock.blockTime());
        byte[] signedHash = parsedBlock.recordFile().signedHash();
        List<RecordFileSignature> verifiedSigs = parsedBlock.signatureFiles().stream()
                .filter(psf -> psf.isValid(signedHash, ab))
                .map(psf -> psf.toRecordFileSignature(ab))
                .toList();

        PreVerifiedBlock preVerified = new PreVerifiedBlock(parsedBlock, ab, verifiedSigs);
        try {
            preVerified = updateAddressBookAndReverify(preVerified, blockNum, addressBookRegistry);
        } catch (Exception e) {
            System.err.printf("[WRAP] Warning: address book auto-update failed at block %d: %s%n", blockNum, e);
        }
        return preVerified;
    }

    private static void runBlockValidations(
            Block wrapped,
            long blockNum,
            List<BlockValidation> parallelValidations,
            List<BlockValidation> sequentialValidations,
            List<BlockValidation> allValidations,
            Path checkpointDir)
            throws Exception {
        Bytes wrappedBytes = Block.PROTOBUF.toBytes(wrapped);
        BlockUnparsed blockUnparsed = BlockUnparsed.PROTOBUF.parse(wrappedBytes);

        for (BlockValidation v : parallelValidations) {
            try {
                v.validate(blockUnparsed, blockNum);
            } catch (ValidationException e) {
                throw new IllegalStateException(
                        "Validation '" + v.name() + "' failed at block " + blockNum + ": " + e.getMessage(), e);
            }
        }
        for (BlockValidation v : sequentialValidations) {
            try {
                v.validate(blockUnparsed, blockNum);
            } catch (ValidationException e) {
                saveValidationCheckpoint(checkpointDir, blockNum - 1, allValidations);
                throw new IllegalStateException(
                        "Validation '" + v.name() + "' failed at block " + blockNum + ": " + e.getMessage(), e);
            }
        }
        for (BlockValidation v : allValidations) {
            v.commitState(blockUnparsed, blockNum);
        }
    }

    /**
     * Saves validation checkpoint state.
     */
    private static void saveValidationCheckpoint(
            Path checkpointDir, long lastValidatedBlockNumber, List<BlockValidation> validations) {
        try {
            Files.createDirectories(checkpointDir);
            // Save progress file so resume can detect checkpoint position
            Path progressFile = checkpointDir.resolve("validateProgress.json");
            String progressJson = "{\"lastValidatedBlockNumber\":" + lastValidatedBlockNumber + "}";
            Files.writeString(progressFile, progressJson, StandardCharsets.UTF_8);
            for (BlockValidation v : validations) {
                try {
                    v.save(checkpointDir);
                } catch (Exception e) {
                    System.err.println("[WRAP] Warning: could not save " + v.name() + " state: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("[WRAP] Warning: could not create checkpoint directory: " + e.getMessage());
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
     * Refreshes GCS listings for a specific day.
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
     * Saves state to JSON file.
     */
    private void saveState(State state) {
        try {
            Path parentDir = stateJsonPath.getParent();
            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }
            String json = GSON.toJson(state);
            Files.writeString(stateJsonPath, json, StandardCharsets.UTF_8);
        } catch (IOException e) {
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
