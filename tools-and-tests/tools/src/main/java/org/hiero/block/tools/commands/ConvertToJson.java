// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands;

import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import com.hedera.hapi.node.base.Transaction;
import com.hedera.hapi.node.transaction.SignedTransaction;
import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Command line command that converts binary block stream files to JSON
 */
@SuppressWarnings({"DataFlowIssue", "unused", "DuplicatedCode", "ConstantValue"})
@Command(name = "json", description = "Converts a binary block stream to JSON")
public class ConvertToJson implements Runnable {

    @Parameters(index = "0..*")
    private File[] files;

    @SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
    @Option(
            names = {"-t", "--transactions"},
            description = "expand transactions")
    private boolean expandTransactions = false;

    @SuppressWarnings({"FieldMayBeFinal", "FieldCanBeLocal"})
    @Option(
            names = {"-ms", "--min-size"},
            description = "minimum file size in megabytes")
    private double minSizeMb = Double.MAX_VALUE;

    /**
     * Empty Default constructor to remove JavaDoc warning
     */
    public ConvertToJson() {}

    /**
     * Main method to run the command
     */
    @Override
    public void run() {
        if (files == null || files.length == 0) {
            System.err.println("No files to convert");
        } else {
            Arrays.stream(files)
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
                    .parallel()
                    .forEach(this::convert);
        }
    }

    /**
     * Converts a binary block stream to JSON
     *
     * @param blockProtoFile the binary block stream file
     */
    private void convert(Path blockProtoFile) {
        final String fileName = blockProtoFile.getFileName().toString();
        final String fileNameNoExt = fileName.substring(0, fileName.lastIndexOf('.'));
        final Path outputFile = blockProtoFile.resolveSibling(fileNameNoExt + ".json");
        try (InputStream fIn = Files.newInputStream(blockProtoFile)) {
            byte[] uncompressedData;
            if (blockProtoFile.getFileName().toString().endsWith(".gz")) {
                uncompressedData = new GZIPInputStream(fIn).readAllBytes();
            } else {
                uncompressedData = fIn.readAllBytes();
            }
            final Block block = Block.PROTOBUF.parse(Bytes.wrap(uncompressedData));
            writeJsonBlock(block, outputFile);
            final long numOfTransactions = block.items().stream()
                    .filter(BlockItem::hasSignedTransaction)
                    .count();
            final String blockNumber =
                    block.items().size() > 1 && block.items().getFirst().hasBlockHeader()
                            ? String.valueOf(Objects.requireNonNull(
                                            block.items().getFirst().blockHeader())
                                    .number())
                            : "unknown";
            System.out.println("Converted \"" + blockProtoFile.getFileName() + "\" "
                    + "Block [" + blockNumber + "] "
                    + "contains = " + block.items().size() + " items, " + numOfTransactions + " transactions");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Converts a parsed block to JSON
     *
     * @param block the block to convert
     * @param expandTransactions whether to expand transactions
     * @return the JSON representation of the block
     */
    public static String toJson(Block block, boolean expandTransactions) {
        if (expandTransactions) {
            String blockJson = Block.JSON.toJSON(block);
            // get iterator over all transactions
            final Iterator<String> transactionBodyJsonIterator = block.items().stream()
                    .filter(BlockItem::hasSignedTransaction)
                    .filter(item -> item.signedTransaction().length() > 0)
                    .map(item -> {
                        try {
                            return "          "
                                    + TransactionBody.JSON
                                            .toJSON(TransactionBody.PROTOBUF.parse(SignedTransaction.PROTOBUF
                                                    .parse(Transaction.PROTOBUF
                                                            .parse(item.signedTransaction())
                                                            .signedTransactionBytes())
                                                    .bodyBytes()))
                                            .replaceAll("\n", "\n          ");
                        } catch (ParseException e) {
                            System.err.println("Error parsing transaction body : " + e.getMessage());
                            throw new RuntimeException(e);
                        }
                    })
                    .iterator();
            // find all "applicationTransaction" fields and expand them, replacing with json from iterator
            return Pattern.compile("(\"applicationTransaction\": )\"([^\"]+)\"")
                    .matcher(blockJson)
                    .replaceAll(matchResult -> matchResult.group(1) + transactionBodyJsonIterator.next());
        } else {
            return Block.JSON.toJSON(block);
        }
    }

    /**
     * Writes a block to a JSON file
     *
     * @param block the block to write
     * @param outputFile the file to write to
     * @throws IOException if an I/O error occurs
     */
    private void writeJsonBlock(Block block, Path outputFile) throws IOException {
        if (expandTransactions) {
            Files.writeString(outputFile, toJson(block, expandTransactions));
        } else {
            try (OutputStream fOut = new BufferedOutputStream(Files.newOutputStream(outputFile), 1024 * 1024)) {
                Block.JSON.write(block, new WritableStreamingData(fOut));
            }
        }
    }
}
