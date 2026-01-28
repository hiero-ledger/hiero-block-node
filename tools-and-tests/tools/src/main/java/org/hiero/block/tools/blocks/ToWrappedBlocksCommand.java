// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.hiero.block.tools.blocks.model.hashing.BlockStreamBlockHasher.hashBlock;
import static org.hiero.block.tools.blocks.model.hashing.HashingUtils.EMPTY_TREE_HASH;
import static org.hiero.block.tools.mirrornode.DayBlockInfo.loadDayBlockInfoMap;
import static org.hiero.block.tools.records.RecordFileDates.FIRST_BLOCK_TIME_INSTANT;

import com.hedera.hapi.block.stream.Block;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.hiero.block.tools.blocks.model.BlockArchiveType;
import org.hiero.block.tools.blocks.model.BlockWriter;
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
 * This class implements the {@link Runnable} interface, allowing multithreaded processing if needed.</p>
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

        // load block times
        try (final BlockTimeReader blockTimeReader = new BlockTimeReader(blockTimesFile);
                // BlockStreamBlockHashRegistry for storing block hashes
                final BlockStreamBlockHashRegistry blockRegistry =
                        new BlockStreamBlockHashRegistry(outputBlocksDir.resolve("blockStreamBlockHashes.bin"))) {
            // get the most recent block number from BlockStreamBlockHashRegistry
            long highestStoredBlockNumber = blockRegistry.highestBlockNumberStored();
            System.out.println(Ansi.AUTO.string("@|yellow Starting from block:|@ " + highestStoredBlockNumber + " @|"));
            // Print highest stored block time
            final Instant highestStoredBlockTime = highestStoredBlockNumber == -1
                    ? FIRST_BLOCK_TIME_INSTANT
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

            // Register a shutdown hook to persist last good status on JVM exit (Ctrl+C, etc.)
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(
                            () -> {
                                System.err.println("Shutdown: address book to " + addressBookFile);
                                addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                // write merkle tree states
                                try {
                                    streamingHasher.save(streamingMerkleTreeFile);
                                    inMemoryTreeHasher.save(inMemoryMerkleTreeFile);
                                    System.err.println("Shutdown: saved merkle tree states. To "
                                            + streamingMerkleTreeFile + " and " + inMemoryMerkleTreeFile);
                                } catch (Exception e) {
                                    System.err.println(
                                            "Failed to save merkle tree states on shutdown: " + e.getMessage());
                                    e.printStackTrace();
                                }
                            },
                            "wrap-shutdown-hook"));

            // track the block number we are working on, atomic as we want to update this global state from lambdas
            final AtomicLong blockCounter = new AtomicLong(startBlock);
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
                    stream
                            // filter out blocks we have already processed, only leaving newer blocks
                            .filter(recordBlock -> recordBlock.recordFileTime().isAfter(highestStoredBlockTime))
                            // parse each record block
                            .map(UnparsedRecordBlock::parse)
                            .forEach(recordBlock -> {
                                try {
                                    final long blockNum = blockCounter.getAndIncrement();
                                    // double-check the blockNum matches one from recordBlock
                                    final long blockNumberFromRecordFile = recordBlock
                                            .recordFile()
                                            .recordStreamFile()
                                            .blockNumber();
                                    if (blockNumberFromRecordFile > 0 && blockNum != blockNumberFromRecordFile) {
                                        throw new RuntimeException("Block number mismatch at "
                                                + recordBlock.blockTime()
                                                + " in "
                                                + dayPath
                                                + ": computed blockNum "
                                                + blockNum
                                                + " != record file block number "
                                                + blockNumberFromRecordFile);
                                    }
                                    // get the block time
                                    final Instant blockTime = blockTimeReader.getBlockInstant(blockNum);
                                    // Convert record file block to wrapped block.
                                    // For block 0, use EMPTY_TREE_HASH for previous block hash since there's no
                                    // previous block. The streamingHasher.computeRootHash() already returns
                                    // EMPTY_TREE_HASH when empty, so allBlocksMerkleTreeRootHash is handled.
                                    // TODO we need to get rid of experimental block, I added experimental to
                                    //  change API locally, We need to push those changes up stream to HAPI lib then
                                    //  pull latest.
                                    final byte[] previousBlockHash =
                                            blockNum == 0 ? EMPTY_TREE_HASH : blockRegistry.mostRecentBlockHash();
                                    final com.hedera.hapi.block.stream.experimental.Block wrappedExp =
                                            RecordBlockConverter.toBlock(
                                                    recordBlock,
                                                    blockNum,
                                                    previousBlockHash,
                                                    streamingHasher.computeRootHash(),
                                                    addressBookRegistry.getAddressBookForBlock(blockTime));

                                    // Convert experimental Block to stable Block for storage APIs
                                    // TODO this will slow things down and can be deleted once above is fixed
                                    final com.hedera.pbj.runtime.io.buffer.Bytes protoBytes =
                                            com.hedera.hapi.block.stream.experimental.Block.PROTOBUF.toBytes(
                                                    wrappedExp);
                                    final Block wrapped = Block.PROTOBUF.parse(protoBytes);
                                    // write the wrapped block to the output directory using the selected archive type
                                    try {
                                        BlockWriter.writeBlock(outputBlocksDir, wrapped, archiveType);
                                    } catch (IOException e) {
                                        PrettyPrint.clearProgress();
                                        System.err.println("Failed writing block " + blockNum + ": " + e.getMessage());
                                        e.printStackTrace();
                                        System.exit(1);
                                    }
                                    // add block hash to merkle tree hashers
                                    final byte[] blockStreamBlockHash =
                                            hashBlock(wrappedExp, streamingHasher.computeRootHash());
                                    streamingHasher.addNodeByHash(blockStreamBlockHash);
                                    inMemoryTreeHasher.addLeaf(blockStreamBlockHash);
                                    // add the block hash to the registry
                                    blockRegistry.addBlock(blockNum, blockStreamBlockHash);

                                    printUpdatedProgress(
                                            recordBlock,
                                            blocksProcessed,
                                            lastSpeedCalcBlockTime,
                                            lastSpeedCalcRealTimeNanos,
                                            blockNum,
                                            startNanos,
                                            totalBlocksToProcess,
                                            lastReportedMinute);
                                } catch (Exception ex) {
                                    PrettyPrint.clearProgress();
                                    System.err.println(
                                            "Failed processing record block in " + dayPath + ": " + ex.getMessage());
                                    ex.printStackTrace();
                                    addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
                                    System.exit(1);
                                }
                            });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            // Clear progress line and print summary
            PrettyPrint.clearProgress();
            System.out.println("Conversion complete. Blocks written: " + blocksProcessed.get());

            addressBookRegistry.saveAddressBookRegistryToJsonFile(addressBookFile);
        } catch (Exception e) {
            throw new RuntimeException(e);
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
}
