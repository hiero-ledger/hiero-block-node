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
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

/**
 * Convert blockchain in record file blocks in day files into wrapped block stream blocks.
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
    private File warningFile = null;

    @Option(
            names = {"-u", "--unzipped"},
            description = "Write output files unzipped, rather than in uncompressed zip batches of 10k ")
    private boolean unzipped = false;

    @Parameters(index = "0", description = "Directory of days to process")
    @SuppressWarnings("unused") // assigned reflectively by picocli
    private File compressedDaysDir;

    @Parameters(index = "1", description = "Directory to write the output wrapped blocks")
    @SuppressWarnings("unused") // assigned reflectively by picocli
    private File outputBlocksDir;

    @Override
    public void run() {
        // create AddressBookRegistry to load address books as needed during conversion
        final AddressBookRegistry addressBookRegistry = new AddressBookRegistry();
        // If inputs are missing, print usage
        if (compressedDaysDir == null || outputBlocksDir == null) {
            spec.commandLine().usage(spec.commandLine().getOut());
            return;
        }

        final List<Path> dayPaths = TarZstdDayUtils.sortedDayPaths(new File[] {compressedDaysDir});
        final AtomicLong blockCounter = new AtomicLong(0L);

        try (FileWriter warningWriter = warningFile != null ? new FileWriter(warningFile, true) : null) {
            for (int dayIndex = 0; dayIndex < dayPaths.size(); dayIndex++) {
                final Path dayPath = dayPaths.get(dayIndex);
                System.out.println("Processing day file: " + dayPath);
                try (var stream = TarZstdDayReaderUsingExec.streamTarZstd(dayPath)) {
                    stream.forEach(recordBlock -> {
                        try {
                            final long blockNum = blockCounter.getAndIncrement();
                            // Convert record file block to wrapped block. We pass zero hashes for previous/root
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
                                            outputBlocksDir.toPath(), blockNum, CompressionType.ZSTD, 3);
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
                                    BlockWriter.writeBlock(outputBlocksDir.toPath(), wrapped);
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
