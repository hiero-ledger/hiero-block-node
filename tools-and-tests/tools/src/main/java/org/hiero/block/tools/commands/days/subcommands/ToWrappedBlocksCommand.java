// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.subcommands;

import com.hedera.hapi.block.stream.Block;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.hiero.block.node.base.BlockFile;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.BlockWriter;
import org.hiero.block.tools.commands.days.model.AddressBookRegistry;
import org.hiero.block.tools.commands.days.model.TarZstdDayReaderUsingExec;
import org.hiero.block.tools.commands.days.model.TarZstdDayUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Convert blockchain in record file blocks in tar.zstd day files into wrapped block stream blocks. This command is
 * designed two work with two directories an input one with day tar.zstd files and an output directory of zip files of
 * wrapped blocks. Optionally the output directory can also contain a "addressBookHistory.json" file which is where this
 * command stores the address books as it builds them processing data.
 * <p>
 * The output format is designed to match the historic storage plugin of Block Node. This should allow the output
 * directory to be dropped in as is into a block node to see it with historical blocks. The Block Node works on
 * individual blocks where each block is a self-contained "Block" protobuf object serialized into a file and zstd
 * compressed. Those compressed blocks are combined into batches by block number into uncompressed zip files. The zip
 * format is used as it reduces stress on OS file system by having fewer files while still allowing random access reads
 * of a single block. At the time of writing Hedera has over 87 million blocks growing by 43,000 a day.
 * </p>
 */
@SuppressWarnings("CallToPrintStackTrace")
@Command(name = "wrap", description = "Convert record file blocks in day files to wrapped block stream blocks")
public class ToWrappedBlocksCommand implements Runnable {

    /** Zero hash for previous / root when none available */
    private static final byte[] ZERO_HASH = new byte[48];

    @Spec
    CommandSpec spec;

    @Option(
            names = {"-w", "--warnings-file"},
            description = "Write warnings to this file, rather than ignoring them")
    private Path warningFile = null;

    @Option(
            names = {"-b", "--blocktimes-file"},
            description = "BlockTimes file for mapping record file times to blocks and back")
    private Path blockTimesFile = Path.of("data/block_times.bin");

    @Option(
            names = {"-u", "--unzipped"},
            description = "Write output files unzipped, rather than in uncompressed zip batches of 10k ")
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
        final AddressBookRegistry addressBookRegistry = new AddressBookRegistry();
        // TODO load address book from outputBlocksDir/addressBookHistory.json
        // If inputs are missing, print usage
        if (compressedDaysDir == null || outputBlocksDir == null) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }

        // check we have a blockTimesFile, create if needed and update it to have latest blocks
        // TODO

        // scan the output dir and work out what the most recent block is so we know where to start
        // TODO

        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new File[] {compressedDaysDir.toFile()});
        // filter lists to only ones that contain next block and newer
        // TODO
        final AtomicLong blockCounter = new AtomicLong(0L);

        try (FileWriter warningWriter = warningFile != null ? new FileWriter(warningFile.toFile(), true) : null) {
            for (int dayIndex = 0; dayIndex < dayPaths.size(); dayIndex++) {
                final Path dayPath = dayPaths.get(dayIndex);
                System.out.println("Processing day file: " + dayPath);
                try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath)) {
                    // if first block scan forward to find block to start at
                    // TODO
                    stream.forEach(recordBlock -> {
                        try {
                            final long blockNum = blockCounter.getAndIncrement();
                            // Convert record file block to wrapped block. We pass zero hashes for previous/root
                            // TODO Rocky we need to get rid of experimental block, I added experimental to change API
                            //  locally, We need to push those changes up stream to HAPI lib then pull latest.
                            final com.hedera.hapi.block.stream.experimental.Block wrappedExp =
                                    recordBlock.toWrappedBlock(
                                            blockNum,
                                            ZERO_HASH,
                                            ZERO_HASH,
                                            addressBookRegistry.getCurrentAddressBook());

                            // Convert experimental Block to stable Block for storage APIs
                            final com.hedera.pbj.runtime.io.buffer.Bytes protoBytes =
                                    com.hedera.hapi.block.stream.experimental.Block.PROTOBUF.toBytes(wrappedExp);
                            final Block wrapped = Block.PROTOBUF.parse(protoBytes);

                            if (unzipped) {
                                try {
                                    final Path outPath = BlockFile.nestedDirectoriesBlockFilePath(
                                            outputBlocksDir, blockNum, CompressionType.ZSTD, 3);
                                    Files.createDirectories(outPath.getParent());
                                    // compress using CompressionType helper and write bytes
                                    final byte[] compressed = CompressionType.ZSTD.compress(protoBytes.toByteArray());
                                    Files.write(outPath, compressed);
                                } catch (IOException e) {
                                    System.err.println(
                                            "Failed writing unzipped block " + blockNum + ": " + e.getMessage());
                                    e.printStackTrace();
                                    System.exit(1);
                                }
                            } else {
                                try {
                                    // BlockWriter will create/append to zip files as needed
                                    BlockWriter.writeBlock(outputBlocksDir, wrapped);
                                } catch (IOException e) {
                                    System.err.println(
                                            "Failed writing zipped block " + blockNum + ": " + e.getMessage());
                                    e.printStackTrace();
                                    System.exit(1);
                                }
                            }
                        } catch (Exception ex) {
                            System.err.println("Failed processing record block in " + dayPath + ": " + ex.getMessage());
                            ex.printStackTrace();
                            System.exit(1);
                        }
                    });
                } catch (Exception ex) {
                    System.err.println("Failed reading day file " + dayPath + ": " + ex.getMessage());
                    ex.printStackTrace();
                    System.exit(1);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Conversion complete. Blocks written: " + blockCounter.get());
    }
}
