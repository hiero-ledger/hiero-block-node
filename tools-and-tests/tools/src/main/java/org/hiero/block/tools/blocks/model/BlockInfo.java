// SPDX-License-Identifier: Apache-2.0
<<<<<<<< HEAD:tools-and-tests/tools/src/main/java/org/hiero/block/tools/blocks/model/BlockInfo.java
package org.hiero.block.tools.blocks.model;
========
package org.hiero.block.tools.blocks;
>>>>>>>> b4a66edb (Clean up of commands structure):tools-and-tests/tools/src/main/java/org/hiero/block/tools/blocks/BlockInfo.java

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.hapi.node.transaction.TransactionBody.DataOneOfType;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.hiero.block.node.base.CompressionType;
import org.hiero.block.tools.blocks.ConvertToJson;

/**
 * Command line command that prints info for block files
 */
@SuppressWarnings({
    "DataFlowIssue",
    "unused",
    "StringConcatenationInsideStringBufferAppend",
    "DuplicatedCode",
    "FieldMayBeFinal"
})
public class BlockInfo {

    /** Pattern to match block file names and extract the block number */
    private static final Pattern BLOCK_FILE_PATTERN = Pattern.compile("^(\\d+)\\.blk(\\.gz|\\.zstd)?$");

    /**
     * Empty Default constructor to remove Javadoc warning
     */
    private BlockInfo() {}

    /**
     * Represents a source for reading a block - either a standalone file or an entry in a zip archive.
     *
     * @param blockNumber the block number extracted from the filename
     * @param filePath the path to the file (either the block file or the zip file)
     * @param zipEntryName the name of the entry within the zip file, or null for standalone files
     * @param compressionType the compression type of the block data
     */
    public record BlockSource(long blockNumber, Path filePath, String zipEntryName, CompressionType compressionType) {
        /** Check if this is a zip entry source. */
        public boolean isZipEntry() {
            return zipEntryName != null;
        }
    }

    /**
     * Extract a block number from a filename matching block file patterns.
     *
     * @param fileName the file name to parse
     * @return the block number, or -1 if not a valid block file name
     */
    private static long extractBlockNumber(String fileName) {
        Matcher matcher = BLOCK_FILE_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        return -1;
    }

    /**
     * Determine the compression type from a file name.
     *
     * @param fileName the file name
     * @return the compression type
     */
    private static CompressionType getCompressionType(String fileName) {
        if (fileName.endsWith(".gz")) {
            return null; // Special case for gzip - handled separately
        } else if (fileName.endsWith(".zstd")) {
            return CompressionType.ZSTD;
        }
        return CompressionType.NONE;
    }

    /**
     * Find all block sources from given files and directories.
     *
     * @param files the input files or directories
     * @param minSizeMb minimum file size filter in MB
     * @return list of block sources sorted by block number
     */
    private static List<BlockSource> findBlockSources(File[] files, double minSizeMb) {
        List<BlockSource> sources = new ArrayList<>();

        Arrays.stream(files)
                .filter(f -> {
                    if (!f.exists()) {
                        System.err.println("File not found : " + f);
                        return false;
                    }
                    return true;
                })
                .map(File::toPath)
                .flatMap(path -> {
                    try {
                        return Files.walk(path);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    String fileName = file.getFileName().toString();

                    // Check for standalone block files (.blk, .blk.gz, .blk.zstd)
                    if (fileName.endsWith(".blk") || fileName.endsWith(".blk.gz") || fileName.endsWith(".blk.zstd")) {
                        long blockNumber = extractBlockNumber(fileName);
                        if (blockNumber >= 0) {
                            try {
                                long fileSize = Files.size(file);
                                if (minSizeMb == Double.MAX_VALUE || fileSize / 1024.0 / 1024.0 >= minSizeMb) {
                                    CompressionType compression = getCompressionType(fileName);
                                    sources.add(new BlockSource(blockNumber, file, null, compression));
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                    // Check for zip files containing blocks
                    else if (fileName.endsWith(".zip")) {
                        try {
                            findBlocksInZip(file, sources, minSizeMb);
                        } catch (IOException e) {
                            System.err.println("Error reading zip file: " + file + " - " + e.getMessage());
                        }
                    }
                });

        // Sort by block number
        sources.sort(Comparator.comparingLong(BlockSource::blockNumber));
        return sources;
    }

    /**
     * Find all blocks within a zip file and add them to the source list.
     *
     * @param zipFile the zip file path
     * @param sources the list to add block sources to
     * @param minSizeMb minimum size filter (applied to compressed size in zip)
     * @throws IOException if an error occurs reading the zip
     */
    private static void findBlocksInZip(Path zipFile, List<BlockSource> sources, double minSizeMb) throws IOException {
        try (FileSystem zipFs = FileSystems.newFileSystem(zipFile);
                Stream<Path> entries = Files.list(zipFs.getPath("/"))) {
            entries.forEach(entry -> {
                String entryName = entry.getFileName().toString();
                if (entryName.endsWith(".blk") || entryName.endsWith(".blk.zstd")) {
                    long blockNumber = extractBlockNumber(entryName);
                    if (blockNumber >= 0) {
                        try {
                            long entrySize = Files.size(entry);
                            if (minSizeMb == Double.MAX_VALUE || entrySize / 1024.0 / 1024.0 >= minSizeMb) {
                                CompressionType compression = getCompressionType(entryName);
                                sources.add(new BlockSource(blockNumber, zipFile, entryName, compression));
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            });
        }
    }

    /**
     * Read block data from a BlockSource.
     *
     * @param source the block source
     * @return array containing [compressedBytes, uncompressedBytes]
     * @throws IOException if an error occurs, reading
     */
    private static byte[][] readBlockData(BlockSource source) throws IOException {
        byte[] compressedBytes;
        byte[] uncompressedBytes;

        if (source.isZipEntry()) {
            // Read from a zip file
            try (FileSystem zipFs = FileSystems.newFileSystem(source.filePath())) {
                Path entryPath = zipFs.getPath("/", source.zipEntryName());
                compressedBytes = Files.readAllBytes(entryPath);
            }
        } else {
            // Read from a standalone file
            compressedBytes = Files.readAllBytes(source.filePath());
        }

        // Decompress based on a compression type
        if (source.compressionType() == null) {
            // Special case for .gz files
            try (InputStream in = new GZIPInputStream(new java.io.ByteArrayInputStream(compressedBytes))) {
                uncompressedBytes = in.readAllBytes();
            }
        } else {
            uncompressedBytes = source.compressionType().decompress(compressedBytes);
        }

        return new byte[][] {compressedBytes, uncompressedBytes};
    }

    /**
     * Produce information for a list of block files
     *
     * @param files the list of block files to produce info for
     * @param csvMode when true, then produce CSV output
     * @param outputFile the output file to write to
     * @param minSizeMb the minimum file size in MB to process
     */
    public static void blockInfo(File[] files, boolean csvMode, File outputFile, double minSizeMb) {
        // atomic counters for total blocks, transactions, items, compressed bytes, and uncompressed bytes
        final AtomicLong totalBlocks = new AtomicLong(0);
        final AtomicLong totalTransactions = new AtomicLong(0);
        final AtomicLong totalItems = new AtomicLong(0);
        final AtomicLong totalBytesCompressed = new AtomicLong(0);
        final AtomicLong totalBytesUncompressed = new AtomicLong(0);
        if (files == null || files.length == 0) {
            System.err.println("No files to display info for");
        } else {
            if (csvMode) {
                System.out.print("Writing CSV output");
            }
            if (outputFile != null) {
                System.out.print("to : " + outputFile.getAbsoluteFile());
            }
            System.out.print("\n");
            totalTransactions.set(0);
            totalItems.set(0);
            totalBytesCompressed.set(0);
            totalBytesUncompressed.set(0);
            // Find all block sources (files and zip entries) sorted by block number
            final List<BlockSource> blockSources = findBlockSources(files, minSizeMb);
            if (blockSources.isEmpty()) {
                System.err.println("No block files found");
                System.exit(1);
            }
            // create a stream of block info strings
            final var blockInfoStream = blockSources.stream()
                    .parallel()
                    .map(source -> blockInfo(
                            source,
                            csvMode,
                            totalBlocks,
                            totalTransactions,
                            totalItems,
                            totalBytesCompressed,
                            totalBytesUncompressed));
            // create a CSV header line
            final String csvHeader = "\"Block\",\"Items\",\"Transactions\",\"Java Objects\","
                    + "\"Original Size (MB)\",\"Uncompressed Size(MB)\",\"Compression\"";
            if (outputFile != null) {
                // check if a file exists and throw an error
                if (outputFile.exists()) {
                    System.err.println("Output file already exists : " + outputFile);
                    System.exit(1);
                }
                AtomicInteger completedFileCount = new AtomicInteger(0);
                try (var writer = Files.newBufferedWriter(outputFile.toPath())) {
                    if (csvMode) {
                        writer.write(csvHeader);
                        writer.newLine();
                    }
                    printProgress(0, blockSources.size(), 0);
                    blockInfoStream.forEachOrdered(line -> {
                        printProgress(
                                (double) completedFileCount.incrementAndGet() / blockSources.size(),
                                blockSources.size(),
                                completedFileCount.get());
                        try {
                            writer.write(line);
                            writer.newLine();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                if (csvMode) {
                    // print CSV column headers
                    System.out.println(csvHeader);
                }
                blockInfoStream.forEachOrdered(System.out::println);
            }
            // print output file complete
            if (outputFile != null) {
                System.out.println("\nOutput written to CSV file: " + outputFile.getAbsoluteFile());
            }
            // print summary
            if (!(csvMode && outputFile != null)) {
                System.out.println("\n=========================================================");
                System.out.println("Summary : ");
                System.out.printf("    Total Blocks                               = %,d \n", totalBlocks.get());
                System.out.printf("    Total Transactions                         = %,d \n", totalTransactions.get());
                System.out.printf("    Total Items                                = %,d \n", totalItems.get());
                System.out.printf(
                        "    Total Bytes Compressed                     = %,.2f MB\n",
                        totalBytesCompressed.get() / 1024.0 / 1024.0);
                System.out.printf(
                        "    Total Bytes Uncompressed                   = %,.2f MB\n",
                        totalBytesUncompressed.get() / 1024.0 / 1024.0);
                System.out.printf(
                        "    Average transactions per block             = %,.2f \n",
                        totalTransactions.get() / (double) totalBlocks.get());
                System.out.printf(
                        "    Average items per transaction              = %,.2f \n",
                        totalItems.get() / (double) totalTransactions.get());
                System.out.printf(
                        "    Average uncompressed bytes per transaction = %,d \n",
                        totalTransactions.get() == 0 ? 0 : (totalBytesUncompressed.get() / totalTransactions.get()));
                System.out.printf(
                        "    Average compressed bytes per transaction   = %,d \n",
                        totalTransactions.get() == 0 ? 0 : totalBytesCompressed.get() / totalTransactions.get());
                System.out.printf(
                        "    Average uncompressed bytes per item        = %,d \n",
                        totalItems.get() == 0 ? 0 : totalBytesUncompressed.get() / totalItems.get());
                System.out.printf(
                        "    Average compressed bytes per item          = %,d \n",
                        totalItems.get() == 0 ? 0 : totalBytesCompressed.get() / totalItems.get());
                System.out.println("=========================================================");
            }
        }
    }

    /**
     * Print progress bar to console
     *
     * @param progress the progress percentage between 0 and 1
     * @param totalBlockFiles the total number of block files
     * @param completedBlockFiles the number of block files completed
     */
    private static void printProgress(double progress, int totalBlockFiles, int completedBlockFiles) {
        final int width = 50;
        System.out.print("\r[");
        int i = 0;
        for (; i <= (int) (progress * width); i++) {
            System.out.print("=");
        }
        for (; i < width; i++) {
            System.out.print(" ");
        }
        System.out.printf(
                "] %.0f%% completed %,d of %,d block files", progress * 100, completedBlockFiles, totalBlockFiles);
    }

    /**
     * Collect info for a block from a BlockSource.
     *
     * @param source the block source to produce info for
     * @param csvMode whether to produce CSV output
     * @param totalBlocks counter for total blocks
     * @param totalTransactions counter for total transactions
     * @param totalItems counter for total items
     * @param totalBytesCompressed counter for total compressed bytes
     * @param totalBytesUncompressed counter for total uncompressed bytes
     * @return the info string
     */
    private static String blockInfo(
            BlockSource source,
            boolean csvMode,
            final AtomicLong totalBlocks,
            final AtomicLong totalTransactions,
            final AtomicLong totalItems,
            final AtomicLong totalBytesCompressed,
            final AtomicLong totalBytesUncompressed) {
        try {
            byte[][] data = readBlockData(source);
            byte[] compressedData = data[0];
            byte[] uncompressedData = data[1];

            long start = System.currentTimeMillis();
            final Block block = Block.PROTOBUF.parse(Bytes.wrap(uncompressedData));
            long end = System.currentTimeMillis();

            return blockInfo(
                    block,
                    end - start,
                    compressedData.length,
                    uncompressedData.length,
                    csvMode,
                    totalBlocks,
                    totalTransactions,
                    totalItems,
                    totalBytesCompressed,
                    totalBytesUncompressed);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            String location = source.isZipEntry()
                    ? source.filePath() + "!" + source.zipEntryName()
                    : source.filePath().toString();
            sw.append("Error processing block " + source.blockNumber() + " from : " + location + "\n");
            e.printStackTrace(new java.io.PrintWriter(sw));
            return sw.toString();
        }
    }

    /**
     * Collect info for a block
     *
     * @param block the block to produce info for
     * @param parseTimeMs the time taken to parse the block in milliseconds
     * @param originalFileSizeBytes the original file size in bytes
     * @param uncompressedFileSizeBytes the uncompressed file size in bytes
     * @return the info string
     */
    private static String blockInfo(
            Block block,
            long parseTimeMs,
            long originalFileSizeBytes,
            long uncompressedFileSizeBytes,
            boolean csvMode,
            final AtomicLong totalBlocks,
            final AtomicLong totalTransactions,
            final AtomicLong totalItems,
            final AtomicLong totalBytesCompressed,
            final AtomicLong totalBytesUncompressed) {
        final StringBuffer output = new StringBuffer();
        long numOfTransactions =
                block.items().stream().filter(BlockItem::hasSignedTransaction).count();
        totalBlocks.incrementAndGet();
        totalTransactions.addAndGet(numOfTransactions);
        totalItems.addAndGet(block.items().size());
        totalBytesCompressed.addAndGet(originalFileSizeBytes);
        totalBytesUncompressed.addAndGet(uncompressedFileSizeBytes);
        String json = ConvertToJson.toJson(block, false);
        // count the number of '{' chars in JSON string to get number of objects
        final long numberOfObjectsInBlock = json.chars().filter(c -> c == '{').count();
        if (!csvMode) {
            output.append(String.format(
                    "Block [%d] contains = %d items, %d transactions, %d java objects : parse time = %d ms\n",
                    block.items().getFirst().blockHeader().number(),
                    block.items().size(),
                    numOfTransactions,
                    numberOfObjectsInBlock,
                    parseTimeMs));
        }

        final double originalFileSizeMb = originalFileSizeBytes / 1024.0 / 1024.0;
        final double uncompressedFileSizeMb = uncompressedFileSizeBytes / 1024.0 / 1024.0;
        final double compressionPercent = 100.0 - (originalFileSizeMb / uncompressedFileSizeMb * 100.0);
        if (!csvMode) {
            output.append(String.format(
                    "    Original File Size = %,.2f MB, Uncompressed File Size = %,.2f MB, Compression = %.2f%%\n",
                    originalFileSizeMb, uncompressedFileSizeMb, compressionPercent));
        }
        Map<String, Long> transactionTypeCounts = new HashMap<>();
        List<String> unknownTransactionInfo = new ArrayList<>();
        AtomicLong numOfSystemTransactions = new AtomicLong();
        block.items().stream()
                .filter(BlockItem::hasSignedTransaction)
                .map(item -> {
                    try {
                        final Transaction transaction = Transaction.PROTOBUF.parse(item.signedTransaction());
                        final TransactionBody transactionBody;
                        if (transaction.signedTransactionBytes().length() > 0) {
                            transactionBody = TransactionBody.PROTOBUF.parse(SignedTransaction.PROTOBUF
                                    .parse(transaction.signedTransactionBytes())
                                    .bodyBytes());
                        } else {
                            transactionBody = TransactionBody.PROTOBUF.parse(transaction.bodyBytes());
                        }
                        final DataOneOfType kind = transactionBody.data().kind();
                        if (kind == DataOneOfType.UNSET) { // should never happen, unless there is a bug somewhere
                            unknownTransactionInfo.add("    " + TransactionBody.JSON.toJSON(transactionBody));
                            unknownTransactionInfo.add("    "
                                    + Transaction.JSON.toJSON(Transaction.PROTOBUF.parse(item.signedTransaction())));
                            unknownTransactionInfo.add("    " + BlockItem.JSON.toJSON(item));
                        } else if (kind == DataOneOfType.STATE_SIGNATURE_TRANSACTION) {
                            numOfSystemTransactions.getAndIncrement();
                        }
                        return kind.toString();
                    } catch (ParseException e) {
                        System.err.println("Error parsing transaction body : " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                })
                .forEach(kind -> transactionTypeCounts.put(kind, transactionTypeCounts.getOrDefault(kind, 0L) + 1));

        // add system transactions to the counts
        if (numOfSystemTransactions.get() > 0) {
            transactionTypeCounts.put("SystemSignature", numOfSystemTransactions.get());
        }

        if (!csvMode) {
            transactionTypeCounts.forEach((k, v) -> output.append(String.format("    %s = %,d transactions\n", k, v)));
            if (!unknownTransactionInfo.isEmpty()) {
                output.append("------------------------------------------\n");
                output.append("    Unknown Transactions : \n");
                unknownTransactionInfo.forEach(
                        info -> output.append("    " + info).append("\n"));
                output.append("------------------------------------------\n");
            }
        } else {

            // print CSV column headers
            output.append(String.format(
                    "\"%d\",\"%d\",\"%d\",\"%d\",\"%.2f\",\"%.2f\",\"%.2f\"",
                    block.items().getFirst().blockHeader().number(),
                    block.items().size(),
                    numOfTransactions,
                    numberOfObjectsInBlock,
                    originalFileSizeMb,
                    uncompressedFileSizeMb,
                    compressionPercent));
        }
        return output.toString();
    }
}
