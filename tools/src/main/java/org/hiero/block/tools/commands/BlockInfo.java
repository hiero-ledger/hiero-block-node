// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands;

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
import java.util.zip.GZIPInputStream;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

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
@Command(name = "info", description = "Prints info for block files")
public class BlockInfo implements Runnable {

    @Parameters(index = "0..*")
    private File[] files;

    @Option(
            names = {"-ms", "--min-size"},
            description = "Filter to only files bigger than this minimum file size in megabytes")
    private double minSizeMb = Double.MAX_VALUE;

    @Option(
            names = {"-c", "--csv"},
            description = "Enable CSV output mode (default: ${DEFAULT-VALUE})")
    private boolean csvMode = false;

    @Option(
            names = {"-o", "--output-file"},
            description = "Output to file rather than stdout")
    private File outputFile;

    // atomic counters for total blocks, transactions, items, compressed bytes, and uncompressed bytes
    private final AtomicLong totalBlocks = new AtomicLong(0);
    private final AtomicLong totalTransactions = new AtomicLong(0);
    private final AtomicLong totalItems = new AtomicLong(0);
    private final AtomicLong totalBytesCompressed = new AtomicLong(0);
    private final AtomicLong totalBytesUncompressed = new AtomicLong(0);

    /**
     * Empty Default constructor to remove JavaDoc warning
     */
    public BlockInfo() {}

    /**
     * Main method to run the command
     */
    @Override
    public void run() {
        System.out.println("csvMode = " + csvMode);
        System.out.println("outputFile = " + outputFile.getAbsoluteFile());
        if (files == null || files.length == 0) {
            System.err.println("No files to convert");
        } else {
            totalTransactions.set(0);
            totalItems.set(0);
            totalBytesCompressed.set(0);
            totalBytesUncompressed.set(0);
            // if none of the files exist then print error message
            if (Arrays.stream(files).noneMatch(File::exists)) {
                System.err.println("No files found");
                System.exit(1);
            }
            // collect all the block file paths sorted by file name
            final List<Path> blockFiles = Arrays.stream(files)
                    .filter(
                            f -> { // filter out non existent files
                                if (!f.exists()) {
                                    System.err.println("File not found : " + f);
                                    return false;
                                } else {
                                    return true;
                                }
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
                    .filter(file -> file.getFileName().toString().endsWith(".blk")
                            || file.getFileName().toString().endsWith(".blk.gz"))
                    .filter(
                            file -> { // handle min file size
                                try {
                                    return minSizeMb == Double.MAX_VALUE
                                            || Files.size(file) / 1024.0 / 1024.0 >= minSizeMb;
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .sorted(Comparator.comparing(file -> file.getFileName().toString()))
                    .toList();
            // create stream of block info strings
            final var blockInfoStream = blockFiles.stream().parallel().map(this::blockInfo);
            // create CSV header line
            final String csvHeader = "\"Block\",\"Items\",\"Transactions\",\"Java Objects\","
                    + "\"Original Size (MB)\",\"Uncompressed Size(MB)\",\"Compression\"";
            if (outputFile != null) {
                // check if file exists and throw error
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
                    printProgress(0, blockFiles.size(), 0);
                    blockInfoStream.forEachOrdered(line -> {
                        printProgress(
                                (double) completedFileCount.incrementAndGet() / blockFiles.size(),
                                blockFiles.size(),
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
    public void printProgress(double progress, int totalBlockFiles, int completedBlockFiles) {
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
     * Collect info for a block file
     *
     * @param blockProtoFile the block file to produce info for
     * @return the info string
     */
    public String blockInfo(Path blockProtoFile) {
        try (InputStream fIn = Files.newInputStream(blockProtoFile)) {
            byte[] uncompressedData;
            if (blockProtoFile.getFileName().toString().endsWith(".gz")) {
                uncompressedData = new GZIPInputStream(fIn).readAllBytes();
            } else {
                uncompressedData = fIn.readAllBytes();
            }
            long start = System.currentTimeMillis();
            final Block block = Block.PROTOBUF.parse(Bytes.wrap(uncompressedData));
            long end = System.currentTimeMillis();
            return blockInfo(block, end - start, Files.size(blockProtoFile), uncompressedData.length);
        } catch (Exception e) {
            StringWriter sw = new StringWriter();
            sw.append("Error processing file : " + blockProtoFile + "\n");
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
    public String blockInfo(Block block, long parseTimeMs, long originalFileSizeBytes, long uncompressedFileSizeBytes) {
        final StringBuffer output = new StringBuffer();
        long numOfTransactions =
                block.items().stream().filter(BlockItem::hasEventTransaction).count();
        totalBlocks.incrementAndGet();
        totalTransactions.addAndGet(numOfTransactions);
        totalItems.addAndGet(block.items().size());
        totalBytesCompressed.addAndGet(originalFileSizeBytes);
        totalBytesUncompressed.addAndGet(uncompressedFileSizeBytes);
        String json = ConvertToJson.toJson(block, false);
        // count number of '{' chars in json string to get number of objects
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
        long numOfSystemTransactions = block.items().stream()
                .filter(BlockItem::hasEventTransaction)
                .filter(item -> item.eventTransaction().hasStateSignatureTransaction())
                .count();
        if (numOfSystemTransactions > 0) {
            transactionTypeCounts.put("SystemSignature", numOfTransactions);
        }
        block.items().stream()
                .filter(BlockItem::hasEventTransaction)
                .map(item -> {
                    if (item.eventTransaction().hasStateSignatureTransaction()) {
                        return "SystemSignature";
                    } else if (item.eventTransaction().hasApplicationTransaction()) {
                        try {
                            final Transaction transaction = Transaction.PROTOBUF.parse(
                                    item.eventTransaction().applicationTransaction());
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
                                        + Transaction.JSON.toJSON(Transaction.PROTOBUF.parse(
                                                item.eventTransaction().applicationTransaction())));
                                unknownTransactionInfo.add("    " + BlockItem.JSON.toJSON(item));
                            }
                            return kind.toString();
                        } catch (ParseException e) {
                            System.err.println("Error parsing transaction body : " + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    } else {
                        unknownTransactionInfo.add("    " + BlockItem.JSON.toJSON(item));
                        return "Unknown";
                    }
                })
                .forEach(kind -> transactionTypeCounts.put(kind, transactionTypeCounts.getOrDefault(kind, 0L) + 1));
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
