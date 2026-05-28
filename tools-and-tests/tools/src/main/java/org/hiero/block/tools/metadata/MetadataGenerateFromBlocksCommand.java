// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.metadata;

import static org.hiero.block.tools.blocks.validation.BlockExtractionUtils.extractBlockInstant;
import static org.hiero.block.tools.records.RecordFileDates.instantToBlockTimeLong;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.stream.Stream;
import org.hiero.block.internal.BlockUnparsed;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command to generate block_times.bin metadata file directly from block files.
 *
 * <p>This command bypasses the Mirror Node REST API and reads block files directly,
 * extracting consensus timestamps from block headers and writing them to the binary
 * metadata file. This is useful when the Mirror Node is unavailable or when working
 * with blocks that haven't been imported yet.
 */
@SuppressWarnings({"FieldCanBeLocal", "FieldMayBeFinal"})
@Command(
        name = "generate-from-blocks",
        description = "Generate block_times.bin metadata directly from block files",
        mixinStandardHelpOptions = true)
public class MetadataGenerateFromBlocksCommand implements Runnable {

    @Parameters(index = "0", description = "Directory containing block files (.blk)")
    private Path blocksDirectory;

    @Option(
            names = {"--block-times"},
            description = "Path to the block times \".bin\" file (default: ${DEFAULT-VALUE})")
    private Path blockTimesFile = MetadataFiles.BLOCK_TIMES_FILE;

    @Option(
            names = {"-v", "--verbose"},
            description = "Print details for each block processed")
    private boolean verbose = false;

    @Override
    public void run() {
        try {
            System.out.println(
                    Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
            System.out.println(Ansi.AUTO.string("@|bold,cyan   GENERATE METADATA FROM BLOCKS|@"));
            System.out.println(
                    Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
            System.out.println();

            // Validate input directory
            if (!Files.exists(blocksDirectory)) {
                System.err.println(
                        Ansi.AUTO.string("@|red Error:|@ Blocks directory does not exist: " + blocksDirectory));
                return;
            }
            if (!Files.isDirectory(blocksDirectory)) {
                System.err.println(Ansi.AUTO.string("@|red Error:|@ Path is not a directory: " + blocksDirectory));
                return;
            }

            // Find all block files
            List<Path> blockFiles;
            try (Stream<Path> paths = Files.walk(blocksDirectory)) {
                blockFiles = paths.filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().endsWith(".blk"))
                        .sorted()
                        .toList();
            }

            if (blockFiles.isEmpty()) {
                System.err.println(Ansi.AUTO.string("@|red Error:|@ No .blk files found in " + blocksDirectory));
                return;
            }

            System.out.println(Ansi.AUTO.string("@|yellow Found|@ " + blockFiles.size() + " @|yellow block files|@"));
            System.out.println();

            // Create output directory if needed
            if (blockTimesFile.getParent() != null) {
                Files.createDirectories(blockTimesFile.getParent());
            }

            // Process blocks and write to file
            int processedCount = 0;
            int errorCount = 0;
            long minBlockNumber = Long.MAX_VALUE;
            long maxBlockNumber = Long.MIN_VALUE;

            try (RandomAccessFile raf = new RandomAccessFile(blockTimesFile.toFile(), "rw")) {
                for (Path blockFile : blockFiles) {
                    try {
                        // Extract block number from filename (e.g., "0000000042.blk" -> 42)
                        String fileName = blockFile.getFileName().toString();
                        long blockNumber = extractBlockNumberFromFilename(fileName);

                        // Read and parse block
                        byte[] blockBytes = Files.readAllBytes(blockFile);
                        BlockUnparsed block = BlockUnparsed.PROTOBUF.parse(Bytes.wrap(blockBytes));

                        // Extract consensus timestamp
                        Instant blockInstant = extractBlockInstant(block);
                        if (blockInstant == null) {
                            System.err.println(Ansi.AUTO.string(
                                    "@|yellow Warning:|@ Block " + blockNumber + " has no header, skipping"));
                            errorCount++;
                            continue;
                        }

                        // Convert to block time long
                        long blockTimeLong = instantToBlockTimeLong(blockInstant);

                        // Write to file at correct position
                        raf.seek(blockNumber * Long.BYTES);
                        raf.writeLong(blockTimeLong);

                        processedCount++;
                        minBlockNumber = Math.min(minBlockNumber, blockNumber);
                        maxBlockNumber = Math.max(maxBlockNumber, blockNumber);

                        if (verbose || processedCount <= 5 || processedCount % 100 == 0) {
                            System.out.println(Ansi.AUTO.string("@|yellow   Block|@ " + blockNumber
                                    + " @|yellow time:|@ " + blockInstant + " @|yellow file:|@ " + fileName));
                        }

                    } catch (Exception e) {
                        System.err.println(Ansi.AUTO.string(
                                "@|yellow Warning:|@ Failed to process " + blockFile + ": " + e.getMessage()));
                        errorCount++;
                    }
                }

                // Flush to disk
                raf.getChannel().force(false);
            }

            System.out.println();
            System.out.println(
                    Ansi.AUTO.string("@|bold,green ════════════════════════════════════════════════════════════|@"));
            System.out.println(Ansi.AUTO.string("@|bold,green   GENERATION COMPLETE|@"));
            System.out.println(
                    Ansi.AUTO.string("@|bold,green ════════════════════════════════════════════════════════════|@"));
            System.out.println(Ansi.AUTO.string("@|green Processed:|@ " + processedCount + " blocks"));
            if (errorCount > 0) {
                System.out.println(Ansi.AUTO.string("@|yellow Errors:|@ " + errorCount + " blocks"));
            }
            System.out.println(Ansi.AUTO.string("@|green Block range:|@ " + minBlockNumber + " to " + maxBlockNumber));
            System.out.println(Ansi.AUTO.string("@|green Output file:|@ " + blockTimesFile));

        } catch (IOException e) {
            System.err.println(Ansi.AUTO.string("@|red Error:|@ " + e.getMessage()));
            e.printStackTrace();
        }
    }

    /**
     * Extract block number from a block filename.
     * Supports formats like: "0000000042.blk", "42.blk", "block_42.blk"
     *
     * @param fileName the block file name
     * @return the block number
     */
    private long extractBlockNumberFromFilename(String fileName) {
        // Remove .blk extension
        String nameWithoutExt = fileName.substring(0, fileName.lastIndexOf(".blk"));

        // Try to extract number from various formats
        // Format 1: "0000000042" or "42"
        try {
            return Long.parseLong(nameWithoutExt);
        } catch (NumberFormatException e) {
            // Try next format
        }

        // Format 2: "block_42" or similar with underscore/hyphen
        String[] parts = nameWithoutExt.split("[_-]");
        for (int i = parts.length - 1; i >= 0; i--) {
            try {
                return Long.parseLong(parts[i]);
            } catch (NumberFormatException e) {
                // Try next part
            }
        }

        throw new IllegalArgumentException("Cannot extract block number from filename: " + fileName);
    }
}
