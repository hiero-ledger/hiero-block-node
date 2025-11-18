// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import com.hedera.hapi.block.stream.Block;
import org.hiero.block.tools.blocks.BlockArchiveType;
import org.hiero.block.tools.blocks.BlockWriter;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;
import org.hiero.block.tools.commands.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.commands.days.model.TarZstdDayUtils;
import org.hiero.block.tools.commands.mirrornode.BlockTimeReader;
import org.hiero.block.tools.commands.mirrornode.DayBlockInfo;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.hiero.block.tools.blocks.BlockWriter.maxStoredBlockNumber;
import static org.hiero.block.tools.commands.mirrornode.DayBlockInfo.loadDayBlockInfoMap;
import static org.hiero.block.tools.commands.mirrornode.UpdateBlockData.updateMirrorNodeData;

/**
 * Convert blockchain in record file blocks in tar.zstd day files into wrapped block stream blocks. This command is
 * designed to work with two directories, an input one with day tar.zstd files and an output directory of zip files of
 * wrapped blocks. Optionally, the output directory can also contain an "addressBookHistory.json" file, which is where
 * this command stores the address books as it builds them processing data.
 * <p>
 * The output format is designed to match the historic storage plugin of Block Node. This should allow the output
 * directory to be dropped in as is into a block node to see it with historical blocks. The Block Node works on
 * individual blocks where each block is a self-contained "Block" protobuf object serialized into a file and zstd
 * compressed. Those compressed blocks are combined into batches by block number into uncompressed zip files. The zip
 * format is used as it reduces stress on an OS file system by having fewer files while still allowing random access
 * reads of a single block. At the time of writing, Hedera has over 87 million blocks growing by 43,000 a day.
 * </p>
 */
@SuppressWarnings({"CallToPrintStackTrace", "FieldCanBeLocal"})
@Command(
        name = "wrap",
        description = "Convert record file blocks in day files to wrapped block stream blocks",
        mixinStandardHelpOptions = true)
public class ToWrappedBlocksCommand implements Runnable {

    /** Zero hash for previous / root when none available */
    private static final byte[] ZERO_HASH = new byte[48];

    @Option(
            names = {"-w", "--warnings-file"},
            description = "Write warnings to this file, rather than ignoring them")
    private Path warningFile = null;

    @Option(
            names = {"-b", "--blocktimes-file"},
            description = "BlockTimes file for mapping record file times to blocks and back")
    private Path blockTimesFile = Path.of("data/block_times.bin");

    /** The path to the day blocks file. */
    @Option(
            names = {"-d", "--day-blocks"},
            description = "Path to the day blocks \".json\" file.")
    private Path dayBlocksFile = DayBlockInfo.DEFAULT_DAY_BLOCKS_PATH;

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

    @Override
    public void run() {
        // create AddressBookRegistry to load address books as needed during conversion
        final Path addressBookFile = outputBlocksDir.resolve("addressBookHistory.json");
        final AddressBookRegistry addressBookRegistry =
                Files.exists(addressBookFile) ? new AddressBookRegistry(addressBookFile) : new AddressBookRegistry();
        // get Archive type
        final BlockArchiveType archiveType = unzipped ? BlockArchiveType.INDIVIDUAL_FILES : BlockArchiveType.UNCOMPRESSED_ZIP;
        // check we have a blockTimesFile, create if needed and update it to have the latest blocks
        if (!Files.exists(blockTimesFile) || !Files.exists(dayBlocksFile)) {
            System.err.println(
                    """
                Missing the data/block_times.bin or day_blocks.json data from mirror node.
                Please use these commands to download:
                   mirror fetchRecordsCsv
                   mirror extractBlockTimes
                   mirror extractDayBlock
                """);
            System.exit(1);
        }
        final long latestBlockNumber = updateMirrorNodeData(blockTimesFile, dayBlocksFile);
        System.out.println(Ansi.AUTO.string("@|yellow Latest block number from mirror node:|@ " + latestBlockNumber));
        // load day block info map
        final Map<LocalDate, DayBlockInfo> dayMap = loadDayBlockInfoMap(dayBlocksFile);
        // load block times
        try (BlockTimeReader blockTimeReader = new BlockTimeReader(blockTimesFile)) {
            // scan the output dir and work out what the most recent block is so we know where to start
            final long highestStoredBlockNumber =
                    maxStoredBlockNumber(outputBlocksDir, BlockWriter.DEFAULT_COMPRESSION);
            final Instant highestStoredBlockTime = highestStoredBlockNumber == -1
                    ? Instant.EPOCH
                    : blockTimeReader.getBlockInstant(highestStoredBlockNumber);
            System.out.println(Ansi.AUTO.string("@|yellow Highest block in block_times.bin:|@ "
                    + highestStoredBlockNumber + " @|yellow at|@ " + highestStoredBlockTime));

            // compute the block to start processing at
            final long startBlock = highestStoredBlockNumber == -1 ? 0 : highestStoredBlockNumber + 1;
            System.out.println(Ansi.AUTO.string("@|yellow Starting from block number:|@ " + startBlock));

            // compute the day that the startBlock is part of
            final LocalDateTime startBlockDateTime = blockTimeReader.getBlockLocalDateTime(startBlock);
            final LocalDate startBlockDate = startBlockDateTime.toLocalDate();
            System.out.println(Ansi.AUTO.string("@|yellow Starting from day:|@ " + startBlockDate));

            // load day paths from the input directory, filtering to just ones newer than the startBlockDate
            final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new File[] {compressedDaysDir.toFile()}).stream()
                    .filter(p -> {
                        final LocalDate fileDate =
                                LocalDate.parse(p.getFileName().toString().substring(0, 10));
                        return fileDate.isEqual(startBlockDate) || fileDate.isAfter(startBlockDate);
                    })
                    .toList();

            // track the block number
            final AtomicLong blockCounter = new AtomicLong(startBlock);
            for (int dayIndex = 0; dayIndex < dayPaths.size(); dayIndex++) {
                final Path dayPath = dayPaths.get(dayIndex);
                final LocalDate dayDate =
                        LocalDate.parse(dayPath.getFileName().toString().substring(0, 10));
                System.out.println(Ansi.AUTO.string("@|yellow Processing day file:|@ " + dayPath));
                long currentBlockNumberBeingRead = dayMap.get(dayDate).firstBlockNumber;
                if (currentBlockNumberBeingRead > startBlock) {
                    // double check blockCounter is in sync
                    if (blockCounter.get() != currentBlockNumberBeingRead) {
                        throw new RuntimeException("Block counter out of sync with day block number for " + dayDate
                                + ": " + blockCounter.get() + " != " + currentBlockNumberBeingRead);
                    }
                }
                try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath)) {
                    stream
                            // filter out blocks we have already processed, only leaving newer blocks
                            .filter(recordBlock -> recordBlock.recordFileTime().isAfter(highestStoredBlockTime))
                            .forEach(recordBlock -> {
                                try {
                                    final long blockNum = blockCounter.getAndIncrement();
                                    // Convert record file block to wrapped block. We pass zero hashes for previous/root
                                    // TODO Rocky we need to get rid of experimental block, I added experimental to
                                    // change API
                                    //  locally, We need to push those changes up stream to HAPI lib then pull latest.
                                    final com.hedera.hapi.block.stream.experimental.Block wrappedExp =
                                            recordBlock.toWrappedBlock(
                                                    blockNum,
                                                    ZERO_HASH,
                                                    ZERO_HASH,
                                                    addressBookRegistry.getCurrentAddressBook());

                                    // Convert experimental Block to stable Block for storage APIs
                                    // TODO Rocky this will slow things down and can be deleted once above is fixed
                                    final com.hedera.pbj.runtime.io.buffer.Bytes protoBytes =
                                            com.hedera.hapi.block.stream.experimental.Block.PROTOBUF.toBytes(
                                                    wrappedExp);
                                    final Block wrapped = Block.PROTOBUF.parse(protoBytes);
                                    // write the wrapped block to the output directory using the selected archive type
                                    try {
                                        BlockWriter.writeBlock(outputBlocksDir, wrapped, archiveType);
                                    } catch (IOException e) {
                                        System.err.println("Failed writing block " + blockNum + ": " + e.getMessage());
                                        e.printStackTrace();
                                        System.exit(1);
                                    }
                                } catch (Exception ex) {
                                    System.err.println(
                                            "Failed processing record block in " + dayPath + ": " + ex.getMessage());
                                    ex.printStackTrace();
                                    System.exit(1);
                                }
                            });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            System.out.println("Conversion complete. Blocks written: " + blockCounter.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
