// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

import static org.hiero.block.tools.mirrornode.MirrorNodeUtils.MAINNET_MIRROR_NODE_API_URL;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Parameters;

/**
 * Command to display block time information for one or more block numbers.
 *
 * <p>For each block number, displays:
 * <ul>
 *   <li>Block time from local block_times.bin file</li>
 *   <li>Block information from mirror node API</li>
 * </ul>
 */
@Command(
        name = "blocktime",
        description = "Display block time information for one or more block numbers",
        mixinStandardHelpOptions = true)
public class BlockTimeCommand implements Runnable {

    private static final DateTimeFormatter DATE_TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"));

    @SuppressWarnings({"unused", "MismatchedQueryAndUpdateOfCollection"})
    @Parameters(paramLabel = "BLOCK_NUMBER", description = "One or more block numbers to query", arity = "1..*")
    private List<Long> blockNumbers;

    /** Default constructor. */
    public BlockTimeCommand() {}

    @Override
    public void run() {
        printHeader();

        try (BlockTimeReader reader = new BlockTimeReader()) {
            long maxBlockNumber = reader.getMaxBlockNumber();

            // Print available block range from block_times.bin
            printAvailableBlockRange(reader, maxBlockNumber);

            for (int i = 0; i < blockNumbers.size(); i++) {
                long blockNumber = blockNumbers.get(i);

                if (i > 0) {
                    printSeparator();
                }

                printBlockInfo(blockNumber, reader, maxBlockNumber);
            }
        } catch (IOException e) {
            System.out.println(Ansi.AUTO.string("@|red ✗ Error reading block_times.bin: " + e.getMessage() + "|@"));
        }

        System.out.println();
    }

    private void printAvailableBlockRange(BlockTimeReader reader, long maxBlockNumber) {
        System.out.println(Ansi.AUTO.string("@|bold,blue ▶ Available Block Range (block_times.bin)|@"));
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────────|@"));
        System.out.println();

        try {
            Instant firstBlockTime = reader.getBlockInstant(0);
            Instant lastBlockTime = reader.getBlockInstant(maxBlockNumber);

            String firstTimeFormatted = DATE_TIME_FORMAT.format(firstBlockTime);
            String lastTimeFormatted = DATE_TIME_FORMAT.format(lastBlockTime);

            System.out.println(Ansi.AUTO.string(String.format(
                    "  @|white First Block:|@ @|yellow %,d|@ @|faint at|@ @|cyan %s UTC|@", 0L, firstTimeFormatted)));
            System.out.println(Ansi.AUTO.string(String.format(
                    "  @|white Last Block:|@  @|yellow %,d|@ @|faint at|@ @|cyan %s UTC|@",
                    maxBlockNumber, lastTimeFormatted)));
            System.out.println(
                    Ansi.AUTO.string(String.format("  @|white Total Blocks:|@ @|bold %,d|@", maxBlockNumber + 1)));
        } catch (Exception e) {
            System.out.println(Ansi.AUTO.string("  @|red ✗ Error reading block range: " + e.getMessage() + "|@"));
        }

        System.out.println();
        printSeparator();
    }

    private void printHeader() {
        System.out.println();
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println(Ansi.AUTO.string("@|bold,cyan   BLOCK TIME INFORMATION|@"));
        System.out.println(
                Ansi.AUTO.string("@|bold,cyan ════════════════════════════════════════════════════════════|@"));
        System.out.println();
    }

    private void printSeparator() {
        System.out.println();
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────────────────|@"));
        System.out.println();
    }

    private void printBlockInfo(long blockNumber, BlockTimeReader reader, long maxBlockNumber) {
        System.out.println(Ansi.AUTO.string("@|bold,blue ▶ Block " + String.format("%,d", blockNumber) + "|@"));
        System.out.println(Ansi.AUTO.string("@|blue ──────────────────────────────────────────────────|@"));
        System.out.println();

        // Get local block time
        printLocalBlockTime(blockNumber, reader, maxBlockNumber);

        // Get mirror node data
        printMirrorNodeData(blockNumber);
    }

    private void printLocalBlockTime(long blockNumber, BlockTimeReader reader, long maxBlockNumber) {
        System.out.println(Ansi.AUTO.string("  @|white Local Block Time (block_times.bin):|@"));

        if (blockNumber < 0) {
            System.out.println(Ansi.AUTO.string("    @|red ✗ Invalid block number (negative)|@"));
            System.out.println();
            return;
        }

        if (blockNumber > maxBlockNumber) {
            System.out.println(Ansi.AUTO.string(String.format(
                    "    @|red ✗ Block number %,d exceeds max available block %,d|@", blockNumber, maxBlockNumber)));
            System.out.println();
            return;
        }

        try {
            Instant blockInstant = reader.getBlockInstant(blockNumber);
            String formattedTime = DATE_TIME_FORMAT.format(blockInstant);

            System.out.println(
                    Ansi.AUTO.string(String.format("    @|green ✓ Timestamp:|@ @|cyan %s UTC|@", formattedTime)));
            System.out.println(Ansi.AUTO.string(
                    String.format("    @|green   Epoch seconds:|@ @|yellow %d|@", blockInstant.getEpochSecond())));
            System.out.println(
                    Ansi.AUTO.string(String.format("    @|green   Nanos:|@ @|yellow %d|@", blockInstant.getNano())));
        } catch (Exception e) {
            System.out.println(Ansi.AUTO.string("    @|red ✗ Error: " + e.getMessage() + "|@"));
        }
        System.out.println();
    }

    private void printMirrorNodeData(long blockNumber) {
        System.out.println(Ansi.AUTO.string("  @|white Mirror Node Data:|@"));

        String url = MAINNET_MIRROR_NODE_API_URL + "blocks/" + blockNumber;
        System.out.println(Ansi.AUTO.string("    @|faint URL: " + url + "|@"));
        System.out.println();

        try {
            JsonObject blockData = MirrorNodeUtils.readUrl(url);

            if (blockData == null) {
                System.out.println(Ansi.AUTO.string("    @|red ✗ No data returned from mirror node|@"));
                return;
            }

            // Extract and display key fields with formatting
            printMirrorNodeField("Block Number", blockData, "number", "yellow");
            printMirrorNodeField("Hash", blockData, "hash", "cyan");
            printMirrorNodeField("Previous Hash", blockData, "previous_hash", "cyan");
            printMirrorNodeField("Timestamp From", blockData, "timestamp.from", "green");
            printMirrorNodeField("Timestamp To", blockData, "timestamp.to", "green");
            printMirrorNodeField("Transaction Count", blockData, "count", "yellow");
            printMirrorNodeField("Size (bytes)", blockData, "size", "yellow");
            printMirrorNodeField("Gas Used", blockData, "gas_used", "yellow");
            printMirrorNodeField("Logs Bloom", blockData, "logs_bloom", "faint");
            printMirrorNodeField("Name", blockData, "name", "white");
        } catch (Exception e) {
            System.out.println(
                    Ansi.AUTO.string("    @|red ✗ Error fetching from mirror node: " + e.getMessage() + "|@"));
        }
    }

    private void printMirrorNodeField(String label, JsonObject data, String fieldPath, String color) {
        String value = getNestedField(data, fieldPath);
        if (value != null && !value.equals("null")) {
            // Truncate long values (like hashes) for display
            String displayValue = value;
            if (value.length() > 80) {
                displayValue = value.substring(0, 40) + "..." + value.substring(value.length() - 20);
            }
            System.out.println(
                    Ansi.AUTO.string(String.format("    @|white %-18s:|@ @|%s %s|@", label, color, displayValue)));
        }
    }

    private String getNestedField(JsonObject data, String fieldPath) {
        String[] parts = fieldPath.split("\\.");
        JsonObject current = data;

        for (int i = 0; i < parts.length - 1; i++) {
            if (current.has(parts[i]) && current.get(parts[i]).isJsonObject()) {
                current = current.getAsJsonObject(parts[i]);
            } else {
                return null;
            }
        }

        String lastField = parts[parts.length - 1];
        if (current.has(lastField)) {
            var element = current.get(lastField);
            if (element.isJsonNull()) {
                return null;
            }
            return element.getAsString();
        }
        return null;
    }
}
