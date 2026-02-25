// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.blocks.AmendmentProvider.createAmendmentProvider;
import static org.hiero.block.tools.blocks.model.BlockWriter.DEFAULT_COMPRESSION;
import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;
import static org.hiero.block.tools.records.RecordFileDates.FIRST_BLOCK_TIME_INSTANT;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.node.base.NodeAddressBook;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.ZipOutputStream;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockWriter;
import org.hiero.block.tools.blocks.model.BlockWriter.BlockPath;
import org.hiero.block.tools.blocks.model.PreVerifiedBlock;
import org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHashRegistry;
import org.hiero.block.tools.blocks.model.hashing.InMemoryTreeHasher;
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
import org.jspecify.annotations.NonNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

/**
 * The {@code ToWrappedBlocksCommand} class is used to convert record file blocks organized in daily files
 * into wrapped block stream files for efficient processing and analysis. This tool is designed to process
 * binary and metadata files required for the conversion process and outputs the resulting wrapped block files
 * in the specified directory.
 * <p>
 * Fields:<br>
 * - {@code blockTimesFile}: Path to the binary file mapping block times to block numbers.<br>
 * - {@code dayBlocksFile}: Path to the JSON file with metadata for blocks organized by day.<br>
 * - {@code unzipped}: Path to the unzipped directory containing the blocks to be processed.<br>
 * - {@code compressedDaysDir}: Path to the directory with compressed daily block files.<br>
 * - {@code outputBlocksDir}: Destination directory for the wrapped block files.
 * </p><p>
 * Processing uses a four-stage pipeline: (1) parse + RSA-verify on a thread pool,
 * (2) convert + chain-state update on the main thread (sequential), (3) serialize + compress on a
 * thread pool, (4) zip write on a single dedicated thread with a long-lived {@link ZipOutputStream}.</p>
 */
@SuppressWarnings({"CallToPrintStackTrace", "FieldCanBeLocal", "DuplicatedCode"})
@Command(
        name = "wrap",
        description = "Convert record file blocks in day files to wrapped block stream blocks",
        mixinStandardHelpOptions = true)
public class ToWrappedBlocksCommand implements Runnable {

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
            names = {"-n", "--network"},
            description = "Network name for applying amendments (mainnet, testnet, none). Default: mainnet")
    private String network = "mainnet";

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
        // check if it exists already, if not, try coping from input dir
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
            System.exit(1);
        }
        // load day block info map
        final Map<LocalDate, DayBlockInfo> dayMap = loadDayBlockInfoMap(dayBlocksFile);

        // Create an amendment provider based on network selection
        final AmendmentProvider amendmentProvider = createAmendmentProvider(network);

        // ---- Pipeline executor services ----
        final int resolvedPrefetch = prefetchSize < 1 ? parseThreads : prefetchSize;
        final ExecutorService parseAndVerifyPool = Executors.newFixedThreadPool(parseThreads);
        final ExecutorService serializePool = Executors.newFixedThreadPool(serializeThreads);
        // Single-threaded so zip writes are strictly ordered and the ZipOutputStream is never
        // accessed by more than one thread at a time.
        final ExecutorService zipWritePool = Executors.newSingleThreadExecutor();

        // Long-lived zip state managed exclusively by zipWritePool.
        // Declared here (outside the try-with-resources) so the shutdown hook can close the zip.
        final AtomicReference<ZipOutputStream> currentZipRef = new AtomicReference<>(null);
        final AtomicReference<Path> currentZipPathRef = new AtomicReference<>(null);

        try ( // load block times
        final BlockTimeReader blockTimeReader = new BlockTimeReader(blockTimesFile);
                // BlockStreamBlockHashRegistry for storing block hashes
                final BlockStreamBlockHashRegistry blockRegistry =
                        new BlockStreamBlockHashRegistry(outputBlocksDir.resolve("blockStreamBlockHashes.bin"))) {
            // get the most recent block number from BlockStreamBlockHashRegistry
            long highestStoredBlockNumber = blockRegistry.highestBlockNumberStored();
            System.out.println(Ansi.AUTO.string("@|yellow Starting from block:|@ " + highestStoredBlockNumber + " @|"));
            // Print highest stored block time
            // Use a time just before the first block so block 0 passes the isAfter filter
            final Instant highestStoredBlockTime = highestStoredBlockNumber == -1
                    ? FIRST_BLOCK_TIME_INSTANT.minusNanos(1)
                    : blockTimeReader.getBlockInstant(highestStoredBlockNumber);
            System.out.println(Ansi.AUTO.string("@|yellow Starting at time:|@ " + highestStoredBlockTime + " @|"));

            // compute the block to start processing at
            final long startBlock = highestStoredBlockNumber == -1 ? 0 : highestStoredBlockNumber + 1;
            System.out.println(Ansi.AUTO.string("@|yellow Starting from block number:|@ " + startBlock));

            // compute the day that the startBlock is part of
            final LocalDateTime startBlockDateTime = blockTimeReader.getBlockLocalDateTime(startBlock);
            final LocalDate startBlockDate = startBlockDateTime.toLocalDate();
            System.out.println(Ansi.AUTO.string("@|yellow Starting from day:|@ " + startBlockDate));

            // load day paths from the input directory, filtering to just ones newer than the startBlockDate and sorting
            final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new File[] {compressedDaysDir.toFile()}).stream()
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

            // create Streaming and In Memory Merkle Tree hashers and load state if files exist
            final Path streamingMerkleTreeFile = outputBlocksDir.resolve("streamingMerkleTree.bin");
            final StreamingHasher streamingHasher = new StreamingHasher();
            if (Files.exists(streamingMerkleTreeFile)) {
                streamingHasher.load(streamingMerkleTreeFile);
            }
            final Path inMemoryMerkleTreeFile = outputBlocksDir.resolve("completeMerkleTree.bin");
            final InMemoryTreeHasher inMemoryTreeHasher = new InMemoryTreeHasher();
            if (Files.exists(inMemoryMerkleTreeFile)) {
                inMemoryTreeHasher.load(inMemoryMerkleTreeFile);
            }
            // File to store jumpstart data (block number, hash, and streaming hasher state)
            final Path jumpstartFile = outputBlocksDir.resolve("jumpstart.bin");
            // Track the last block number and hash in memory, write once at the end
            final AtomicLong jumpstartBlockNumber = new AtomicLong(-1);
            final AtomicReference<byte[]> jumpstartBlockHash = new AtomicReference<>(null);

            // Register a shutdown hook to persist last good status on JVM exit (Ctrl+C, etc.)
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(
                            () -> {
                                if (!Files.isDirectory(outputBlocksDir)) {
                                    System.err.println("Shutdown: output directory no longer exists, skipping save: "
                                            + outputBlocksDir);
                                    return;
                                }
                                // Interrupt pipeline workers so they stop accepting new work
                                parseAndVerifyPool.shutdownNow();
                                serializePool.shutdownNow();
                                zipWritePool.shutdownNow();
                                // Close the current open zip (if any) to flush completed entries
                                final ZipOutputStream openZip = currentZipRef.get();
                                if (openZip != null) {
                                    try {
                                        openZip.close();
                                    } catch (IOException ignored) {
                                    }
                                }
                                try {
                                    System.err.println("Shutdown: address book to " + addressBookFile);
                                    addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                    streamingHasher.save(streamingMerkleTreeFile);
                                    inMemoryTreeHasher.save(inMemoryMerkleTreeFile);
                                    // Save jumpstart data if we processed any blocks
                                    if (jumpstartBlockHash.get() != null) {
                                        saveJumpstartData(
                                                jumpstartFile,
                                                jumpstartBlockNumber.get(),
                                                jumpstartBlockHash.get(),
                                                streamingHasher);
                                    }
                                    System.err.println("Shutdown: saved merkle tree states. To "
                                            + streamingMerkleTreeFile + " and " + inMemoryMerkleTreeFile);
                                } catch (Exception e) {
                                    System.err.println("Shutdown: could not save state: " + e.getMessage());
                                }
                            },
                            "wrap-shutdown-hook"));

            // track the block number we are working on, atomic as we want to update this global state from lambdas
            final AtomicLong blockCounter = new AtomicLong(startBlock);

            // lastWriteFuture chains all zip-write tasks sequentially across days.
            // It starts as an already-completed future so the first block's write can proceed immediately.
            CompletableFuture<Void> lastWriteFuture = CompletableFuture.completedFuture(null);

            // Iterate over all the days to convert. We have to convert in order and sequentially as we are building new
            // ordered blockchains
            for (final Path dayPath : dayPaths) {
                final LocalDate dayDate = dayPathToLocalDate(dayPath);
                long currentBlockNumberBeingRead = dayMap.get(dayDate).firstBlockNumber;
                System.out.println(Ansi.AUTO.string("\n@|yellow Starting processing day:|@ " + dayPath
                        + " @|yellow at block:|@ " + currentBlockNumberBeingRead + " @|"));
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

                    while (it.hasNext() || !parseWindow.isEmpty()) {
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
                            ex.printStackTrace();
                            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                            System.exit(1);
                            return; // unreachable – silences "preVerified may be uninitialized" warning
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
                        //   • block N-1's zip write is complete before block N is written
                        //     (sequential appends to the same open ZipOutputStream)
                        //   • block N's bytes are ready before we try to write them
                        final CompletableFuture<Void> prevWrite = lastWriteFuture;
                        lastWriteFuture = CompletableFuture.allOf(prevWrite, serFuture)
                                .thenRunAsync(
                                        () -> {
                                            try {
                                                final byte[] bytes = serFuture.join();
                                                if (archiveType == BlockArchiveType.UNCOMPRESSED_ZIP) {
                                                    // Switch zip file when the block number crosses into a new range
                                                    if (!blockPath.zipFilePath().equals(currentZipPathRef.get())) {
                                                        final ZipOutputStream old = currentZipRef.get();
                                                        if (old != null) {
                                                            old.close();
                                                        }
                                                        currentZipPathRef.set(blockPath.zipFilePath());
                                                        currentZipRef.set(
                                                                BlockWriter.openZipForAppend(blockPath.zipFilePath()));
                                                    }
                                                    BlockWriter.writeBlockEntry(currentZipRef.get(), blockPath, bytes);
                                                } else {
                                                    // Individual-file mode: each block is its own file
                                                    Files.write(blockPath.zipFilePath(), bytes);
                                                }
                                            } catch (IOException e) {
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
            }

            // After all days are processed: close the last open zip file and wait for every
            // pending zip-write task to finish before saving chain state.
            lastWriteFuture
                    .thenRunAsync(
                            () -> {
                                try {
                                    final ZipOutputStream last = currentZipRef.get();
                                    if (last != null) {
                                        last.close();
                                        currentZipRef.set(null);
                                    }
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            },
                            zipWritePool)
                    .join();

            // Clear progress line and print summary
            PrettyPrint.clearProgress();
            System.out.println("Conversion complete. Blocks written: " + blocksProcessed.get());

            // Save jumpstart data once at the end
            if (jumpstartBlockHash.get() != null) {
                saveJumpstartData(jumpstartFile, jumpstartBlockNumber.get(), jumpstartBlockHash.get(), streamingHasher);
            }

            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            parseAndVerifyPool.shutdownNow();
            serializePool.shutdownNow();
            zipWritePool.shutdownNow();
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
     *   <li>Streaming hasher pending subtree hashes (48 bytes × hash count)</li>
     * </ul>
     *
     * @param file the file to write to
     * @param blockNumber the block number
     * @param blockHash the block hash (SHA-384, 48 bytes) - this is the previous block root hash
     * @param streamingHasher the streaming hasher containing the merkle tree state
     */
    private static void saveJumpstartData(
            Path file, long blockNumber, byte[] blockHash, StreamingHasher streamingHasher) {
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
