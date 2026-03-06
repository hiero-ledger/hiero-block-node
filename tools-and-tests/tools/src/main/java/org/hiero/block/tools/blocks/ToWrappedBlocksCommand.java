// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.blocks.AmendmentProvider.createAmendmentProvider;
import static org.hiero.block.tools.blocks.HasherStateFiles.saveStateCheckpoint;
import static org.hiero.block.tools.blocks.model.BlockWriter.DEFAULT_COMPRESSION;
import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;
import static org.hiero.block.tools.records.RecordFileDates.FIRST_BLOCK_TIME_INSTANT;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockPath;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockZipAppender;
import org.hiero.block.tools.blocks.model.PreVerifiedBlock;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
import org.hiero.block.tools.blocks.model.hashing.StreamingHasher;
import org.hiero.block.tools.config.NetworkConfig;
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
import org.jspecify.annotations.NonNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * Converts record file blocks organized in daily {@code .tar.zstd} archives into wrapped block
 * stream files compatible with the Hiero Block Node historic file format.
 *
 * <h2>Four-Stage Processing Pipeline</h2>
 *
 * <p>Processing uses a concurrent pipeline to maximise throughput. Stages are connected by
 * {@link CompletableFuture} chains. The block-hash chain constrains Stage 2 to remain sequential,
 * but all other stages run concurrently across multiple blocks.</p>
 *
 * <pre>
 * Tar stream (main thread, sequential I/O)
 *     |
 *     v
 * Stage 1 — Parse + RSA-Verify  [parseAndVerifyPool, N threads, sliding prefetch window]
 *
 *   For each UnparsedRecordBlock:
 *     1. unparsed.parse()  -&gt;  ParsedRecordBlock
 *     2. addressBookRegistry.getAddressBookForBlock(parsed.blockTime())
 *     3. parsed.signatureFiles().stream().parallel()
 *            .filter(psf -&gt; psf.isValid(signedHash, addressBook))   // RSA verify
 *            .map(psf  -&gt; psf.toRecordFileSignature(addressBook))
 *     -&gt;  PreVerifiedBlock(recordBlock, addressBook, verifiedSignatures)
 *
 *     future.join() — main thread waits for the oldest in-flight future
 *     |
 *     v
 * Stage 2 — Convert + Chain-State Update  [main thread, strictly sequential]
 *
 *   RecordBlockConverter.toBlock(preVerified, blockNum, prevHash, treeRoot, amendments)
 *   hashBlock(wrapped)  -&gt;  blockStreamBlockHash
 *   streamingHasher.addNodeByHash(hash)       // NOT thread-safe; stays on main thread
 *   inMemoryTreeHasher.addNodeByHash(hash)    // NOT thread-safe; stays on main thread
 *   blockRegistry.addBlock(blockNum, hash)    // NOT thread-safe; stays on main thread
 *   BlockWriter.computeBlockPath(...)         // pre-compute path + createDirectories
 *     |
 *     +-- supplyAsync(serializePool) --------------------+
 *     |                                                  |
 *     v                                                  v
 * Stage 3 — Serialize + Compress  [serializePool, N threads]
 *
 *   BlockWriter.serializeBlockToBytes(wrapped, compressionType)
 *   PBJ protobuf serialization + optional zstd compression
 *   Multiple blocks compressed concurrently on all available cores.
 *     |
 *     | allOf(prevWriteFuture, serFuture).thenRunAsync(zipWritePool)
 *     v
 * Stage 4 — Zip Write  [zipWritePool, 1 thread, long-lived ZipOutputStream]
 *
 *   One ZipOutputStream is kept open across many consecutive blocks.
 *   It is closed and re-opened only when the block number crosses into a
 *   new 10,000-block zip-file range (i.e. blockPath.zipFilePath() changes).
 *   BlockWriter.writeBlockEntry(zip, blockPath, bytes)  -&gt;  CRC32 + putNextEntry + write
 * </pre>
 *
 * <p>The {@code allOf(prevWriteFuture, serFuture)} dependency in Stage 4 guarantees:</p>
 * <ul>
 *   <li>Block N-1's append completes before block N is written to the same open stream.</li>
 *   <li>Block N's compressed bytes are available before the write begins.</li>
 * </ul>
 *
 * <h2>Durability</h2>
 * <p>A durable commit watermark ({@code wrap-commit.bin}) tracks the highest block number whose
 * zip write has completed. On crash, the registry and hashers are truncated/rebuilt to the
 * watermark, ensuring no block is claimed as written when its zip entry may be incomplete.</p>
 *
 * <h2>Thread-Safety Constraints</h2>
 * <ul>
 *   <li>{@link StreamingHasher}, {@link InMemoryTreeHasher}, and
 *       {@link BlockStreamBlockHashRegistry} are <em>not</em> thread-safe — all updates
 *       are performed on the Stage 2 (main) thread only.</li>
 *   <li>The single zip appender held by Stage 4 is never shared across threads;
 *       only {@code zipWritePool}'s single thread accesses it.</li>
 *   <li>{@link AddressBookRegistry}, {@link RecordBlockConverter}, and
 *       {@link BlockWriter#serializeBlockToBytes} are safe to call from multiple threads.</li>
 * </ul>
 *
 * <h2>CLI Options</h2>
 * <ul>
 *   <li>{@code --parse-threads} — Stage 1 thread count (default: CPU count - 1)</li>
 *   <li>{@code --serialize-threads} — Stage 3 thread count (default: CPU count - 1)</li>
 *   <li>{@code --prefetch} — sliding parse-ahead window size (default: same as
 *       {@code --parse-threads})</li>
 * </ul>
 */
@SuppressWarnings("FieldCanBeLocal")
@Command(
        name = "wrap",
        description = "Convert record file blocks in day files to wrapped block stream blocks",
        mixinStandardHelpOptions = true)
public class ToWrappedBlocksCommand implements Runnable {

    /** Name of the durable commit watermark file inside the output directory. */
    private static final String WATERMARK_FILE_NAME = "wrap-commit.bin";

    /** Write the watermark every this many blocks (plus on zip close and shutdown). */
    private static final int WATERMARK_BATCH_SIZE = 256;

    @Option(
            names = {"-b", "--blocktimes-file"},
            description = "BlockTimes file for mapping record file times to blocks and back")
    private Path blockTimesFile = MetadataFiles.BLOCK_TIMES_FILE;

    /** The path to the day blocks file. */
    @Option(
            names = {"-d", "--day-blocks"},
            description = "Path to the day blocks \".json\" file.")
    private Path dayBlocksFile = MetadataFiles.DAY_BLOCKS_FILE;

    @Option(
            names = {"-u", "--unzipped"},
            description =
                    "Write output files as individual files in nested directories, rather than in uncompressed zip batches of 10k ")
    private boolean unzipped = false;

    @Option(
            names = {"-i", "--input-dir"},
            description = "Directory of record file tar.zstd days to process")
    private Path compressedDaysDir = Path.of("compressedDays");

    @Option(
            names = {"-o", "--output-dir"},
            description = "Directory to write the output wrapped blocks")
    @SuppressWarnings("unused") // assigned reflectively by picocli
    private Path outputBlocksDir = Path.of("wrappedBlocks");

    @Option(
            names = {"--parse-threads"},
            description = "Thread count for the parse + RSA-verify stage. Default: CPU count minus 1")
    private int parseThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    @Option(
            names = {"--serialize-threads"},
            description = "Thread count for the block serialization + compression stage. Default: CPU count minus 1")
    private int serializeThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

    @Option(
            names = {"--prefetch"},
            description = "Number of parse+verify futures to keep in-flight ahead of the convert thread. "
                    + "Default: same as --parse-threads")
    private int prefetchSize = -1;

    /** Set by the shutdown hook to request an orderly drain of the pipeline. */
    private volatile boolean shutdownRequested = false;

    /**
     * Run the ToWrappedBlocksCommand to convert record file blocks in day files to wrapped block stream blocks.
     */
    @Override
    public void run() {
        // create an output directory if it does not exist
        try {
            Files.createDirectories(outputBlocksDir);
            System.out.println(
                    Ansi.AUTO.string("@|yellow Created new output directory:|@ " + outputBlocksDir.toAbsolutePath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // create AddressBookRegistry to load address books as needed during conversion
        final Path addressBookFile = outputBlocksDir.resolve("addressBookHistory.json");
        // check if it exists already, if not, try copying from input dir
        if (!Files.exists(addressBookFile)) {
            final Path inputAddressBookFile = compressedDaysDir.resolve("addressBookHistory.json");
            if (Files.exists(inputAddressBookFile)) {
                try {
                    Files.copy(inputAddressBookFile, addressBookFile);
                    System.out.println(Ansi.AUTO.string("@|yellow Copied existing address book history to output:|@ "
                            + addressBookFile.toAbsolutePath()));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        // load or create a new AddressBookRegistry
        final AddressBookRegistry addressBookRegistry =
                Files.exists(addressBookFile) ? new AddressBookRegistry(addressBookFile) : new AddressBookRegistry();
        System.out.println(
                Ansi.AUTO.string("@|yellow Loaded address book registry:|@ \n" + addressBookRegistry.toPrettyString()));
        // get Archive type
        final BlockArchiveType archiveType =
                unzipped ? BlockArchiveType.INDIVIDUAL_FILES : BlockArchiveType.UNCOMPRESSED_ZIP;
        // check we have a blockTimesFile
        if (!Files.exists(blockTimesFile) || !Files.exists(dayBlocksFile)) {
            System.err.println("""
                Missing the data/block_times.bin or day_blocks.json data from mirror node.
                Please use these commands to download:
                   mirror fetchRecordsCsv
                   mirror extractBlockTimes
                   mirror extractDayBlock
                """);
            return;
        }
        // load day block info map
        final Map<LocalDate, DayBlockInfo> dayMap = loadDayBlockInfoMap(dayBlocksFile);

        // Create an amendment provider based on network selection
        final AmendmentProvider amendmentProvider =
                createAmendmentProvider(NetworkConfig.current().networkName());

        // ---- Pipeline executor services ----
        final int resolvedPrefetch = prefetchSize < 1 ? parseThreads : prefetchSize;
        final ExecutorService parseAndVerifyPool = Executors.newFixedThreadPool(parseThreads);
        final ExecutorService serializePool = Executors.newFixedThreadPool(serializeThreads);
        // Single-threaded so zip writes are strictly ordered and the ZipOutputStream is never
        // accessed by more than one thread at a time.
        final ExecutorService zipWritePool = Executors.newSingleThreadExecutor();

        // Long-lived zip state managed exclusively by zipWritePool.
        final AtomicReference<BlockZipAppender> currentZipRef = new AtomicReference<>(null);
        final AtomicReference<Path> currentZipPathRef = new AtomicReference<>(null);

        // Durable watermark: highest block whose zip write completed. Updated on zipWritePool.
        final Path watermarkFile = outputBlocksDir.resolve(WATERMARK_FILE_NAME);
        final AtomicLong durableWatermark = new AtomicLong(loadWatermark(watermarkFile));

        // Track how many blocks have been written since the last watermark flush
        final AtomicLong blocksSinceWatermarkFlush = new AtomicLong(0);

        try ( // load block times
        final BlockTimeReader blockTimeReader = new BlockTimeReader(blockTimesFile);
                // BlockStreamBlockHashRegistry for storing block hashes
                final BlockStreamBlockHashRegistry blockRegistry =
                        new BlockStreamBlockHashRegistry(outputBlocksDir.resolve("blockStreamBlockHashes.bin"))) {

            // ---- Watermark-based resume reconciliation ----
            final long watermark = durableWatermark.get();
            final long registryHighest = blockRegistry.highestBlockNumberStored();

            // If registry is ahead of the watermark, truncate it back. Blocks beyond the
            // watermark may not have been fully written to zip files before the crash.
            if (watermark >= 0 && registryHighest > watermark) {
                System.out.println("Registry has blocks up to " + registryHighest + " but watermark is at " + watermark
                        + "; truncating registry.");
                blockRegistry.truncateTo(watermark);
            } else if (watermark < 0 && registryHighest >= 0) {
                // No watermark file but registry has data — trust registry as-is for
                // backwards compatibility with runs before watermark was introduced.
                System.out.println("No watermark file found; trusting registry at block " + registryHighest);
                durableWatermark.set(registryHighest);
            }

            final long effectiveHighest = blockRegistry.highestBlockNumberStored();
            System.out.println(Ansi.AUTO.string("@|yellow Starting from block:|@ " + effectiveHighest));

            // Use a time just before the first block so block 0 passes the isAfter filter
            final Instant highestStoredBlockTime = effectiveHighest == -1
                    ? FIRST_BLOCK_TIME_INSTANT.minusNanos(1)
                    : blockTimeReader.getBlockInstant(effectiveHighest);
            System.out.println(Ansi.AUTO.string("@|yellow Starting at time:|@ " + highestStoredBlockTime));

            // compute the block to start processing at
            final long startBlock = effectiveHighest == -1 ? 0 : effectiveHighest + 1;
            System.out.println(Ansi.AUTO.string("@|yellow Starting from block number:|@ " + startBlock));

            // compute the day that the startBlock is part of
            final LocalDateTime startBlockDateTime = blockTimeReader.getBlockLocalDateTime(startBlock);
            final LocalDate startBlockDate = startBlockDateTime.toLocalDate();
            System.out.println(Ansi.AUTO.string("@|yellow Starting from day:|@ " + startBlockDate));

            // Create fresh hashers and replay all hashes from registry.
            // StreamingHasher and InMemoryTreeHasher cannot go backwards, so on resume we
            // always rebuild from the authoritative registry rather than loading stale state files.
            final Path streamingMerkleTreeFile = outputBlocksDir.resolve("streamingMerkleTree.bin");
            final StreamingHasher streamingHasher = new StreamingHasher();

            final Path inMemoryMerkleTreeFile = outputBlocksDir.resolve("completeMerkleTree.bin");
            final InMemoryTreeHasher inMemoryTreeHasher = new InMemoryTreeHasher();

            if (effectiveHighest >= 0) {
                System.out.println("Replaying " + (effectiveHighest + 1) + " block hashes into hashers (blocks 0.."
                        + effectiveHighest + ")");
                for (long bn = 0; bn <= effectiveHighest; bn++) {
                    final byte[] hash = blockRegistry.getBlockHash(bn);
                    streamingHasher.addNodeByHash(hash);
                    inMemoryTreeHasher.addNodeByHash(hash);
                }
                System.out.println("Hasher replay complete. Streaming leafCount=" + streamingHasher.leafCount()
                        + ", inMemory leafCount=" + inMemoryTreeHasher.leafCount());
            }

            // load day paths from the input directory, filtering to just ones newer than the startBlockDate and sorting
            final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new Path[] {compressedDaysDir}).stream()
                    .filter(p -> {
                        final LocalDate fileDate = dayPathToLocalDate(p);
                        return fileDate.isEqual(startBlockDate) || fileDate.isAfter(startBlockDate);
                    })
                    .sorted(Comparator.comparingLong(p -> dayPathToLocalDate(p).toEpochDay()))
                    .toList();
            // print range of days to be processed
            if (dayPaths.isEmpty()) {
                System.out.println(Ansi.AUTO.string("@|yellow No day files to process after:|@ " + startBlockDate));
                return;
            } else {
                System.out.println(Ansi.AUTO.string("@|yellow Processing day files from|@ "
                        + dayPathToLocalDate(dayPaths.getFirst())
                        + " @|yellow to|@ "
                        + dayPathToLocalDate(dayPaths.getLast())));
            }

            // Progress tracking setup
            final long startNanos = System.nanoTime();
            // Calculate total blocks to process from the last day's info
            final DayBlockInfo lastDayInfo = dayMap.get(dayPathToLocalDate(dayPaths.getLast()));
            final long totalBlocksToProcess = lastDayInfo != null ? lastDayInfo.lastBlockNumber - startBlock + 1 : 0;
            final AtomicLong blocksProcessed = new AtomicLong(0);
            // Track last block time for speed calculation
            final AtomicReference<Instant> lastSpeedCalcBlockTime = new AtomicReference<>();
            final AtomicLong lastSpeedCalcRealTimeNanos = new AtomicLong(0);
            // Track the last reported minute to avoid spamming progress output
            final AtomicLong lastReportedMinute = new AtomicLong(Long.MIN_VALUE);

            // File to store jumpstart data (block number, hash, and streaming hasher state)
            final Path jumpstartFile = outputBlocksDir.resolve("jumpstart.bin");
            // Track the last block number and hash in memory, write once at the end
            final AtomicLong jumpstartBlockNumber = new AtomicLong(-1);
            final AtomicReference<byte[]> jumpstartBlockHash = new AtomicReference<>(null);

            // Register a shutdown hook to request orderly drain on JVM exit (Ctrl+C, etc.)
            final Thread shutdownHook = new Thread(
                    () -> {
                        if (!Files.isDirectory(outputBlocksDir)) {
                            System.err.println(
                                    "Shutdown: output directory no longer exists, skipping save: " + outputBlocksDir);
                            return;
                        }
                        // Signal the main loop to stop after the current block
                        shutdownRequested = true;

                        // Wait for the zip-write pool to drain pending work (up to 30 seconds)
                        zipWritePool.shutdown();
                        try {
                            if (!zipWritePool.awaitTermination(30, TimeUnit.SECONDS)) {
                                System.err.println("Shutdown: zip write pool did not drain in 30s, forcing.");
                                zipWritePool.shutdownNow();
                            }
                        } catch (InterruptedException ignored) {
                            zipWritePool.shutdownNow();
                        }

                        // Save watermark after zip pool has drained
                        saveWatermark(watermarkFile, durableWatermark.get());

                        // Shut down other pools
                        parseAndVerifyPool.shutdownNow();
                        serializePool.shutdownNow();

                        try {
                            System.err.println("Shutdown: saving address book to " + addressBookFile);
                            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                        } catch (Exception e) {
                            System.err.println("Shutdown: could not save address book: " + e.getMessage());
                        }
                        saveStateCheckpoint(
                                streamingMerkleTreeFile, streamingHasher,
                                inMemoryMerkleTreeFile, inMemoryTreeHasher);
                        System.err.println("Shutdown: saved merkle tree states to " + streamingMerkleTreeFile + " and "
                                + inMemoryMerkleTreeFile);
                        // Save jumpstart data if we processed any blocks
                        if (jumpstartBlockHash.get() != null) {
                            saveJumpstartData(
                                    jumpstartFile,
                                    jumpstartBlockNumber.get(),
                                    jumpstartBlockHash.get(),
                                    streamingHasher);
                        }
                    },
                    "wrap-shutdown-hook");
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            // track the block number we are working on, atomic as we want to update this global state from lambdas
            final AtomicLong blockCounter = new AtomicLong(startBlock);

            // lastWriteFuture chains all zip-write tasks sequentially across days.
            // It starts as an already-completed future so the first block's write can proceed immediately.
            CompletableFuture<Void> lastWriteFuture = CompletableFuture.completedFuture(null);

            // Track the last calendar month (year*12+month) for which a state checkpoint was saved.
            // Initialised to -1 so that the first block's month does not trigger a premature save.
            int lastSavedBlockMonth = -1;

            // Track whether a parse failure occurred so we can throw after drain
            String parseFailureMessage = null;

            // Iterate over all the days to convert. We have to convert in order and sequentially as we are building new
            // ordered blockchains
            for (final Path dayPath : dayPaths) {
                if (shutdownRequested) {
                    break;
                }
                final LocalDate dayDate = dayPathToLocalDate(dayPath);
                long currentBlockNumberBeingRead = dayMap.get(dayDate).firstBlockNumber;
                PrettyPrint.clearProgress();
                System.out.println(Ansi.AUTO.string("@|yellow Starting processing day:|@ " + dayPath
                        + " @|yellow at block:|@ " + currentBlockNumberBeingRead));
                if (currentBlockNumberBeingRead > startBlock) {
                    // double-check blockCounter is in sync
                    if (blockCounter.get() != currentBlockNumberBeingRead) {
                        throw new RuntimeException("Block counter out of sync with day block number for " + dayDate
                                + ": " + blockCounter.get() + " != " + currentBlockNumberBeingRead);
                    }
                }
                // read the record stream blocks from the day tar.zstd file
                try (Stream<UnparsedRecordBlock> stream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath)) {
                    // Sliding window of Stage 1 (parse + RSA-verify) futures
                    final Deque<CompletableFuture<PreVerifiedBlock>> parseWindow = new ArrayDeque<>();
                    final Iterator<UnparsedRecordBlock> it = stream.filter(
                                    recordBlock -> recordBlock.recordFileTime().isAfter(highestStoredBlockTime))
                            .iterator();

                    while ((it.hasNext() || !parseWindow.isEmpty()) && !shutdownRequested) {
                        // ---- Stage 1: fill the sliding parse+verify window ----
                        // Submit up to resolvedPrefetch parse+RSA-verify tasks concurrently.
                        while (parseWindow.size() < resolvedPrefetch && it.hasNext()) {
                            final UnparsedRecordBlock unparsed = it.next();
                            parseWindow.add(CompletableFuture.supplyAsync(
                                    () -> {
                                        final ParsedRecordBlock parsed = unparsed.parse();
                                        final NodeAddressBook ab =
                                                addressBookRegistry.getAddressBookForBlock(parsed.blockTime());
                                        final byte[] signedHash =
                                                parsed.recordFile().signedHash();
                                        final List<RecordFileSignature> sigs = parsed.signatureFiles().stream()
                                                .parallel()
                                                .filter(psf -> psf.isValid(signedHash, ab))
                                                .map(psf -> psf.toRecordFileSignature(ab))
                                                .toList();
                                        return new PreVerifiedBlock(parsed, ab, sigs);
                                    },
                                    parseAndVerifyPool));
                        }
                        if (parseWindow.isEmpty()) {
                            break;
                        }

                        // ---- Stage 2: convert + chain-state update (main thread, sequential) ----
                        // Wait for the oldest in-flight parse+verify future.
                        final PreVerifiedBlock preVerified;
                        try {
                            preVerified = parseWindow.poll().join();
                        } catch (Exception ex) {
                            PrettyPrint.clearProgress();
                            System.err.println("Failed parsing/verifying block in " + dayPath + ": " + ex.getMessage());
                            ex.printStackTrace(System.err);
                            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                            parseFailureMessage =
                                    "Failed parsing/verifying block in " + dayPath + ": " + ex.getMessage();
                            shutdownRequested = true;
                            break;
                        }

                        final long blockNum = blockCounter.getAndIncrement();
                        // double-check the blockNum matches one from recordBlock
                        final long blockNumberFromRecordFile = preVerified
                                .recordBlock()
                                .recordFile()
                                .recordStreamFile()
                                .blockNumber();
                        if (blockNumberFromRecordFile > 0 && blockNum != blockNumberFromRecordFile) {
                            throw new RuntimeException("Block number mismatch at "
                                    + preVerified.recordBlock().blockTime()
                                    + " in "
                                    + dayPath
                                    + ": computed blockNum "
                                    + blockNum
                                    + " != record file block number "
                                    + blockNumberFromRecordFile);
                        }

                        // Monthly checkpoint: save state once per calendar month of blockchain data.
                        // Worst-case on corruption: re-process at most ~1 month of blocks.
                        // Check BEFORE updating chain state so the saved state is consistent with
                        // all zip writes for blocks up to (but not including) this block.
                        final var blockDateTime =
                                preVerified.recordBlock().blockTime().atOffset(ZoneOffset.UTC);
                        final int blockMonth = blockDateTime.getYear() * 12 + blockDateTime.getMonthValue();
                        if (lastSavedBlockMonth >= 0 && blockMonth != lastSavedBlockMonth) {
                            // Wait for all preceding zip writes before snapshotting state.
                            lastWriteFuture.join();
                            saveStateCheckpoint(
                                    streamingMerkleTreeFile, streamingHasher,
                                    inMemoryMerkleTreeFile, inMemoryTreeHasher);
                            System.out.println("Monthly checkpoint saved before block " + blockNum);
                        }
                        lastSavedBlockMonth = blockMonth;

                        // Convert record file block to wrapped block using pre-verified signatures
                        final Block wrapped = RecordBlockConverter.toBlock(
                                preVerified,
                                blockNum,
                                blockRegistry.mostRecentBlockHash(),
                                streamingHasher.computeRootHash(),
                                amendmentProvider);

                        // Update chain state (not thread-safe – must stay on main thread)
                        final byte[] blockStreamBlockHash = hashBlock(wrapped);
                        streamingHasher.addNodeByHash(blockStreamBlockHash);
                        inMemoryTreeHasher.addNodeByHash(blockStreamBlockHash);
                        blockRegistry.addBlock(blockNum, blockStreamBlockHash);
                        jumpstartBlockNumber.set(blockNum);
                        jumpstartBlockHash.set(blockStreamBlockHash);

                        // Pre-compute block path on the convert thread (pure arithmetic, fast).
                        // Creating the directory here avoids doing it on the zip-write thread.
                        final BlockPath blockPath =
                                BlockWriter.computeBlockPath(outputBlocksDir, blockNum, archiveType);
                        Files.createDirectories(blockPath.dirPath());

                        // ---- Stage 3: serialize + compress in parallel ----
                        final CompletableFuture<byte[]> serFuture = CompletableFuture.supplyAsync(
                                () -> BlockWriter.serializeBlockToBytes(wrapped, DEFAULT_COMPRESSION), serializePool);

                        // ---- Stage 4: zip write on the single-threaded zipWritePool ----
                        // allOf(prevWrite, serFuture) ensures:
                        //   block N-1's zip write is complete before block N is written
                        //   (sequential appends to the same open ZipOutputStream)
                        //   block N's bytes are ready before we try to write them
                        final CompletableFuture<Void> prevWrite = lastWriteFuture;
                        lastWriteFuture = CompletableFuture.allOf(prevWrite, serFuture)
                                .thenRunAsync(
                                        () -> {
                                            try {
                                                final byte[] bytes = serFuture.join();
                                                if (archiveType == BlockArchiveType.UNCOMPRESSED_ZIP) {
                                                    // Switch zip file when the block number crosses into a new range
                                                    if (!blockPath.zipFilePath().equals(currentZipPathRef.get())) {
                                                        final BlockZipAppender old = currentZipRef.get();
                                                        if (old != null) {
                                                            old.close();
                                                            // Flush watermark on zip-file close
                                                            saveWatermark(watermarkFile, durableWatermark.get());
                                                            blocksSinceWatermarkFlush.set(0);
                                                        }
                                                        // Clear ref before open so a failure leaves clean null state
                                                        // rather than a stale/closed appender
                                                        currentZipRef.set(null);
                                                        currentZipPathRef.set(null);
                                                        final BlockZipAppender newAppender =
                                                                BlockWriter.openZipForAppend(blockPath.zipFilePath());
                                                        currentZipRef.set(newAppender);
                                                        currentZipPathRef.set(blockPath.zipFilePath());
                                                    }
                                                    BlockWriter.writeBlockEntry(currentZipRef.get(), blockPath, bytes);
                                                } else {
                                                    // Individual-file mode: each block is its own file
                                                    Files.write(blockPath.zipFilePath(), bytes);
                                                }
                                                // Update watermark after successful write
                                                durableWatermark.set(blockNum);
                                                // Flush watermark periodically
                                                if (blocksSinceWatermarkFlush.incrementAndGet()
                                                        >= WATERMARK_BATCH_SIZE) {
                                                    saveWatermark(watermarkFile, blockNum);
                                                    blocksSinceWatermarkFlush.set(0);
                                                }
                                            } catch (IOException e) {
                                                // Close the zip appender to avoid resource leak
                                                final BlockZipAppender leakedZip = currentZipRef.getAndSet(null);
                                                currentZipPathRef.set(null);
                                                if (leakedZip != null) {
                                                    try {
                                                        leakedZip.close();
                                                    } catch (IOException suppressed) {
                                                        e.addSuppressed(suppressed);
                                                    }
                                                }
                                                throw new UncheckedIOException(e);
                                            }
                                        },
                                        zipWritePool);

                        printUpdatedProgress(
                                preVerified.recordBlock(),
                                blocksProcessed,
                                lastSpeedCalcBlockTime,
                                lastSpeedCalcRealTimeNanos,
                                blockNum,
                                startNanos,
                                totalBlocksToProcess,
                                lastReportedMinute);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                if (shutdownRequested) {
                    break;
                }
            }

            // After all days are processed (or shutdown requested): close the last open zip file
            // and wait for every pending zip-write task to finish before saving chain state.
            // The zip is always closed on its own thread to avoid cross-thread access.
            lastWriteFuture
                    .thenRunAsync(
                            () -> {
                                try {
                                    final BlockZipAppender last = currentZipRef.get();
                                    if (last != null) {
                                        last.close();
                                        currentZipRef.set(null);
                                    }
                                    // Final watermark flush
                                    saveWatermark(watermarkFile, durableWatermark.get());
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            },
                            zipWritePool)
                    .join();

            // Clear progress line and print summary
            PrettyPrint.clearProgress();
            System.out.println("Conversion complete. Blocks written: " + blocksProcessed.get());

            // Remove shutdown hook before saving state to prevent concurrent writes
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down — hook is running or has run
            }

            // Save hasher states atomically now that all writes are complete.
            saveStateCheckpoint(
                    streamingMerkleTreeFile, streamingHasher,
                    inMemoryMerkleTreeFile, inMemoryTreeHasher);

            // Save jumpstart data once at the end
            if (jumpstartBlockHash.get() != null) {
                saveJumpstartData(jumpstartFile, jumpstartBlockNumber.get(), jumpstartBlockHash.get(), streamingHasher);
            }

            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);

            // If we stopped due to a parse failure, throw after saving state
            if (parseFailureMessage != null) {
                throw new RuntimeException(parseFailureMessage);
            }
        } catch (Exception e) {
            throw (e instanceof RuntimeException re) ? re : new RuntimeException(e);
        } finally {
            parseAndVerifyPool.shutdown();
            serializePool.shutdown();
            zipWritePool.shutdown();
            try {
                parseAndVerifyPool.awaitTermination(10, TimeUnit.SECONDS);
                serializePool.awaitTermination(10, TimeUnit.SECONDS);
                zipWritePool.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Load the durable commit watermark from the given file.
     *
     * @param watermarkFile the path to the watermark file
     * @return the watermark block number, or {@code -1} if the file does not exist or is invalid
     */
    static long loadWatermark(Path watermarkFile) {
        if (!Files.exists(watermarkFile)) {
            return -1;
        }
        try {
            final byte[] bytes = Files.readAllBytes(watermarkFile);
            if (bytes.length < Long.BYTES) {
                return -1;
            }
            return ByteBuffer.wrap(bytes).getLong();
        } catch (IOException e) {
            System.err.println("Warning: could not read watermark file: " + e.getMessage());
            return -1;
        }
    }

    /**
     * Save the durable commit watermark atomically (write to .tmp, then rename).
     *
     * @param watermarkFile the path to the watermark file
     * @param blockNumber the highest block number durably written
     */
    static void saveWatermark(Path watermarkFile, long blockNumber) {
        if (blockNumber < 0) {
            return;
        }
        final Path tmpFile = watermarkFile.resolveSibling(WATERMARK_FILE_NAME + ".tmp");
        try {
            final byte[] bytes =
                    ByteBuffer.allocate(Long.BYTES).putLong(blockNumber).array();
            Files.write(tmpFile, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tmpFile, watermarkFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            System.err.println("Warning: could not save watermark: " + e.getMessage());
        }
    }

    /**
     * Convert a day file path to a LocalDate.
     *
     * @param p the day file path to a directory with a name in the form "2025-11-13"
     * @return return the LocalDate corresponding to the day file path
     */
    private static @NonNull LocalDate dayPathToLocalDate(Path p) {
        return LocalDate.parse(p.getFileName().toString().substring(0, 10));
    }

    /**
     * Print updated progress information including processing speed and ETA.
     *
     * @param recordBlock the parsed record block
     * @param blocksProcessed the number of blocks processed so far
     * @param lastSpeedCalcBlockTime last speed calculation block time
     * @param lastSpeedCalcRealTimeNanos last speed calculation real time nanos
     * @param blockNum the block number being processed
     * @param startNanos the start time of the conversion process in nanos
     * @param totalBlocksToProcess the total number of blocks to process
     * @param lastReportedMinute the last reported minute to avoid spamming progress output
     */
    private static void printUpdatedProgress(
            ParsedRecordBlock recordBlock,
            AtomicLong blocksProcessed,
            AtomicReference<Instant> lastSpeedCalcBlockTime,
            AtomicLong lastSpeedCalcRealTimeNanos,
            long blockNum,
            long startNanos,
            long totalBlocksToProcess,
            AtomicLong lastReportedMinute) {
        // Update progress tracking
        blocksProcessed.incrementAndGet();

        // Calculate processing speed over the last 10 seconds of wall clock time
        final long currentRealTimeNanos = System.nanoTime();
        final long tenSecondsInNanos = 10_000_000_000L;
        String speedString = "";

        // Initialize tracking on the first block
        if (lastSpeedCalcBlockTime.get() == null) {
            lastSpeedCalcBlockTime.set(recordBlock.blockTime());
            lastSpeedCalcRealTimeNanos.set(currentRealTimeNanos);
        }

        // Update the tracking window if more than 10 seconds of real time has elapsed
        long realTimeSinceLastCalc = currentRealTimeNanos - lastSpeedCalcRealTimeNanos.get();
        if (realTimeSinceLastCalc >= tenSecondsInNanos) {
            lastSpeedCalcBlockTime.set(recordBlock.blockTime());
            lastSpeedCalcRealTimeNanos.set(currentRealTimeNanos);
        }

        // Calculate speed if we have at least 1 second of real time elapsed since tracking
        // point
        if (realTimeSinceLastCalc >= 1_000_000_000L) { // At least 1 second
            long dataTimeElapsedMillis = recordBlock.blockTime().toEpochMilli()
                    - lastSpeedCalcBlockTime.get().toEpochMilli();
            long realTimeElapsedMillis = realTimeSinceLastCalc / 1_000_000L;
            double speedMultiplier = (double) dataTimeElapsedMillis / (double) realTimeElapsedMillis;
            speedString = String.format(" speed %.1fx", speedMultiplier);
        }

        // Build progress string
        final String progressString = String.format("Block %d at %s%s", blockNum, recordBlock.blockTime(), speedString);

        // Calculate ETA
        final long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
        final long processedCount = blocksProcessed.get();
        double percent = ((double) processedCount / (double) totalBlocksToProcess) * 100.0;
        long remainingMillis =
                PrettyPrint.computeRemainingMilliseconds(processedCount, totalBlocksToProcess, elapsedMillis);

        // Only print progress once per consensus-minute to avoid spam
        long blockMinute = recordBlock.blockTime().getEpochSecond() / 60L;
        if (blockMinute != lastReportedMinute.get()) {
            PrettyPrint.printProgressWithEta(percent, progressString, remainingMillis);
            lastReportedMinute.set(blockMinute);
        }
    }

    /**
     * Saves jumpstart data to a binary file. This provides the Consensus Node with all data
     * needed to continue block processing from where the wrap command left off.
     *
     * <p>File format:
     * <ul>
     *   <li>Block number (8 bytes, long)</li>
     *   <li>Previous block root hash (48 bytes, SHA-384)</li>
     *   <li>Streaming hasher leaf count (8 bytes, long)</li>
     *   <li>Streaming hasher hash count (4 bytes, int)</li>
     *   <li>Streaming hasher pending subtree hashes (48 bytes x hash count)</li>
     * </ul>
     *
     * @param file the file to write to
     * @param blockNumber the block number
     * @param blockHash the block hash (SHA-384, 48 bytes) - this is the previous block root hash
     * @param streamingHasher the streaming hasher containing the merkle tree state
     */
    static void saveJumpstartData(Path file, long blockNumber, byte[] blockHash, StreamingHasher streamingHasher) {
        try (DataOutputStream out = new DataOutputStream(
                Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))) {
            // Block number and hash (previous block root hash for the next block)
            out.writeLong(blockNumber);
            out.write(blockHash);

            // Streaming hasher state for subtree 2 continuation
            out.writeLong(streamingHasher.leafCount());
            List<byte[]> hashList = streamingHasher.intermediateHashingState();
            out.writeInt(hashList.size());
            for (byte[] hash : hashList) {
                out.write(hash);
            }
        } catch (IOException e) {
            System.err.println("Warning: could not save jumpstart.bin: " + e.getMessage());
        }
    }
}
